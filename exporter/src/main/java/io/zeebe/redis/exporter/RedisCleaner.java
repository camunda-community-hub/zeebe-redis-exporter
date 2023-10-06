package io.zeebe.redis.exporter;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCleaner {

    private final static String CLEANUP_LOCK = "zeebe:cleanup-lock";
    private final static String CLEANUP_TIMESTAMP = "zeebe:cleanup-time";
    private final Logger logger;
    private StatefulRedisConnection<String, ?> redisConnection;

    private Map<String, Boolean> streams = new ConcurrentHashMap<>();

    private boolean useProtoBuf;
    private long maxTtlInMillisConfig;
    private long minTtlInMillisConfig;
    private boolean deleteAfterAcknowledge;

    private Duration trimScheduleDelay;

    public RedisCleaner(StatefulRedisConnection<String, ?> redisConnection, boolean useProtoBuf, ExporterConfiguration config, Logger logger) {
        this.logger = logger;
        this.redisConnection = redisConnection;
        this.useProtoBuf = useProtoBuf;
        minTtlInMillisConfig = config.getMinTimeToLiveInSeconds() * 1000l;
        if (minTtlInMillisConfig < 0) minTtlInMillisConfig = 0;
        maxTtlInMillisConfig = config.getMaxTimeToLiveInSeconds() * 1000l;
        if (maxTtlInMillisConfig < 0) maxTtlInMillisConfig = 0;
        deleteAfterAcknowledge = config.isDeleteAfterAcknowledge();
        trimScheduleDelay = Duration.ofSeconds(config.getCleanupCycleInSeconds());
    }

    public void considerStream(String stream) {
        streams.put(stream, Boolean.TRUE);
    }

    public void trimStreamValues() {
        if (redisConnection != null && streams.size() > 0 && acquireCleanupLock()) {
            try {
                // get ID according to max time to live
                final long maxTTLMillis = System.currentTimeMillis() - maxTtlInMillisConfig;
                final String maxTTLId = String.valueOf(maxTTLMillis);
                // get ID according to min time to live
                final long minTTLMillis = System.currentTimeMillis() - minTtlInMillisConfig;
                final String minTTLId = String.valueOf(minTTLMillis);
                logger.debug("trim streams {}", streams);
                // trim all streams
                List<String> keys = new ArrayList(streams.keySet());
                keys.forEach(stream -> {
                    Optional<Long> minDelivered = !deleteAfterAcknowledge ? Optional.empty() :
                            redisConnection.sync().xinfoGroups(stream)
                                    .stream().map(o -> XInfoGroup.fromXInfo(o, useProtoBuf))
                                    .map(xi -> {
                                        if (xi.getPending() > 0) {
                                            xi.considerPendingMessageId(redisConnection.sync()
                                                    .xpending(stream, xi.getName()).getMessageIds().getLower().getValue());
                                        }
                                        return xi.getLastDeliveredId();
                                    })
                                    .min(Comparator.comparing(Long::longValue));
                    if (minDelivered.isPresent()) {
                        long minDeliveredMillis = minDelivered.get();
                        String xtrimMinId = String.valueOf(minDeliveredMillis);
                        if (maxTtlInMillisConfig > 0 && maxTTLMillis > minDeliveredMillis) {
                            xtrimMinId = maxTTLId;
                        } else if (minTtlInMillisConfig > 0 && minTTLMillis < minDeliveredMillis) {
                            xtrimMinId = minTTLId;
                        }
                        long numTrimmed = redisConnection.sync().xtrim(stream, new XTrimArgs().minId(xtrimMinId));
                        if (numTrimmed > 0) {
                            logger.debug("{}: {} cleaned records", stream, numTrimmed);
                        }
                    } else if (maxTtlInMillisConfig > 0) {
                        long numTrimmed = redisConnection.sync().xtrim(stream, new XTrimArgs().minId(maxTTLId));
                        if (numTrimmed > 0) {
                            logger.debug("{}: {} cleaned records", stream, numTrimmed);
                        }
                    }
                });
            } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
                logger.error("Error during cleanup due to possible Redis unavailability: {}", ex.getMessage());
            } catch (Exception ex) {
                logger.error("Error during cleanup", ex);
            } finally {
                releaseCleanupLock();
            }
        }
    }

    private boolean acquireCleanupLock() {
        try {
            String id = UUID.randomUUID().toString();
            Long now = System.currentTimeMillis();
            // ProtoBuf format
            if (useProtoBuf) {
                // try to get lock
                StatefulRedisConnection<String, byte[]> con = (StatefulRedisConnection<String, byte[]>) redisConnection;
                con.sync().set(CLEANUP_LOCK, id.getBytes(StandardCharsets.UTF_8), SetArgs.Builder.nx().px(trimScheduleDelay));
                byte[] getResult = con.sync().get(CLEANUP_LOCK);
                if (getResult != null && getResult.length > 0 && id.equals(new String(getResult, StandardCharsets.UTF_8))) {
                    // lock successful: check last cleanup timestamp (autoscaled new Zeebe instances etc.)
                    byte[] lastCleanup = con.sync().get(CLEANUP_TIMESTAMP);
                    if (lastCleanup == null || lastCleanup.length == 0 ||
                            Long.parseLong(new String(lastCleanup, StandardCharsets.UTF_8)) < now - trimScheduleDelay.toMillis()) {
                        con.sync().set(CLEANUP_TIMESTAMP, Long.toString(now).getBytes(StandardCharsets.UTF_8));
                        return true;
                    }
                }
                // JSON format
            } else {
                // try to get lock
                StatefulRedisConnection<String, String> con = (StatefulRedisConnection<String, String>) redisConnection;
                con.sync().set(CLEANUP_LOCK, id, SetArgs.Builder.nx().px(trimScheduleDelay));
                if (id.equals(con.sync().get(CLEANUP_LOCK))) {
                    // lock successful: check last cleanup timestamp (autoscaled new Zeebe instances etc.)
                    String lastCleanup = con.sync().get(CLEANUP_TIMESTAMP);
                    if (lastCleanup == null || lastCleanup.isEmpty() || Long.parseLong(lastCleanup) < now - trimScheduleDelay.toMillis()) {
                        con.sync().set(CLEANUP_TIMESTAMP, Long.toString(now));
                        return true;
                    }
                }
            }
        } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
            logger.error("Error acquiring cleanup lock due to possible Redis unavailability: {}", ex.getMessage());
        } catch (Exception ex) {
            logger.error("Error acquiring cleanup lock", ex);
        }
        return false;
    }

    private void releaseCleanupLock() {
        try {
            redisConnection.sync().del(CLEANUP_LOCK);
        } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
            logger.error("Error releasing cleanup lock due to possible Redis unavailability: {}", ex.getMessage());
        } catch (Exception ex) {
            logger.error("Error releasing cleanup lock", ex);
        }
    }
}
