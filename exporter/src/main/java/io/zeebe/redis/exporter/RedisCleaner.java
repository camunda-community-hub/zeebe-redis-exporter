package io.zeebe.redis.exporter;

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.SetArgs;
import io.lettuce.core.XTrimArgs;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;

public class RedisCleaner {

  private static final String CLEANUP_LOCK = "zeebe:cleanup-lock";
  private static final String CLEANUP_TIMESTAMP = "zeebe:cleanup-time";
  private final Logger logger;
  private UniversalRedisConnection<String, ?> redisConnection;

  private Map<String, Boolean> streams = new ConcurrentHashMap<>();

  private boolean useProtoBuf;
  private long maxTtlInMillisConfig;
  private long minTtlInMillisConfig;
  private boolean deleteAfterAcknowledge;

  private Duration trimScheduleDelay;

  public RedisCleaner(
      UniversalRedisConnection<String, ?> redisConnection,
      boolean useProtoBuf,
      ExporterConfiguration config,
      Logger logger) {
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

  public void setRedisConnection(UniversalRedisConnection<String, ?> redisConnection) {
    this.redisConnection = redisConnection;
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
        RedisStreamCommands<String, ?> streamCommands = redisConnection.syncStreamCommands();
        keys.forEach(
            stream -> {
              Optional<Long> minDelivered =
                  !deleteAfterAcknowledge
                      ? Optional.empty()
                      : streamCommands.xinfoGroups(stream).stream()
                          .map(o -> XInfoGroup.fromXInfo(o, useProtoBuf))
                          .map(
                              xi -> {
                                if (xi.getPending() > 0) {
                                  xi.considerPendingMessageId(
                                      streamCommands
                                          .xpending(stream, xi.getName())
                                          .getMessageIds()
                                          .getLower()
                                          .getValue());
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
                long numTrimmed = streamCommands.xtrim(stream, new XTrimArgs().minId(xtrimMinId));
                if (numTrimmed > 0) {
                  logger.debug("{}: {} cleaned records", stream, numTrimmed);
                }
              } else if (maxTtlInMillisConfig > 0) {
                long numTrimmed = streamCommands.xtrim(stream, new XTrimArgs().minId(maxTTLId));
                if (numTrimmed > 0) {
                  logger.debug("{}: {} cleaned records", stream, numTrimmed);
                }
              }
            });
      } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
        logger.error(
            "Error during cleanup due to possible Redis unavailability: {}", ex.getMessage());
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
        RedisStringCommands<String, byte[]> stringCommands =
            (RedisStringCommands<String, byte[]>) redisConnection.syncStringCommands();
        stringCommands.set(
            CLEANUP_LOCK,
            id.getBytes(StandardCharsets.UTF_8),
            SetArgs.Builder.nx().px(trimScheduleDelay));
        byte[] getResult = stringCommands.get(CLEANUP_LOCK);
        if (getResult != null
            && getResult.length > 0
            && id.equals(new String(getResult, StandardCharsets.UTF_8))) {
          // lock successful: check last cleanup timestamp (autoscaled new Zeebe instances etc.)
          byte[] lastCleanup = stringCommands.get(CLEANUP_TIMESTAMP);
          if (lastCleanup == null
              || lastCleanup.length == 0
              || Long.parseLong(new String(lastCleanup, StandardCharsets.UTF_8))
                  < now - trimScheduleDelay.toMillis()) {
            stringCommands.set(
                CLEANUP_TIMESTAMP, Long.toString(now).getBytes(StandardCharsets.UTF_8));
            return true;
          }
        }
        // JSON format
      } else {
        // try to get lock
        RedisStringCommands<String, String> stringCommands =
            (RedisStringCommands<String, String>) redisConnection.syncStringCommands();
        stringCommands.set(CLEANUP_LOCK, id, SetArgs.Builder.nx().px(trimScheduleDelay));
        if (id.equals(stringCommands.get(CLEANUP_LOCK))) {
          // lock successful: check last cleanup timestamp (autoscaled new Zeebe instances etc.)
          String lastCleanup = stringCommands.get(CLEANUP_TIMESTAMP);
          if (lastCleanup == null
              || lastCleanup.isEmpty()
              || Long.parseLong(lastCleanup) < now - trimScheduleDelay.toMillis()) {
            stringCommands.set(CLEANUP_TIMESTAMP, Long.toString(now));
            return true;
          }
        }
      }
    } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
      logger.error(
          "Error acquiring cleanup lock due to possible Redis unavailability: {}", ex.getMessage());
    } catch (Exception ex) {
      logger.error("Error acquiring cleanup lock", ex);
    }
    return false;
  }

  private void releaseCleanupLock() {
    try {
      redisConnection.syncDel(CLEANUP_LOCK);
    } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
      logger.error(
          "Error releasing cleanup lock due to possible Redis unavailability: {}", ex.getMessage());
    } catch (Exception ex) {
      logger.error("Error releasing cleanup lock", ex);
    }
  }
}
