package io.zeebe.redis.exporter;

import io.lettuce.core.*;
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
  private long consumerJobTimeout;
  private long consumerIdleTimeout;

  private Duration trimScheduleDelay;

  private String streamPrefix;
  private long keyScanCycle;
  private long lastKeyScan = -1;

  public RedisCleaner(
      UniversalRedisConnection<String, ?> redisConnection,
      boolean useProtoBuf,
      ExporterConfiguration config,
      Logger logger) {
    this.logger = logger;
    this.redisConnection = redisConnection;
    this.useProtoBuf = useProtoBuf;
    minTtlInMillisConfig = config.getMinTimeToLiveInSeconds() * 1000L;
    if (minTtlInMillisConfig < 0) minTtlInMillisConfig = 0;
    maxTtlInMillisConfig = config.getMaxTimeToLiveInSeconds() * 1000L;
    if (maxTtlInMillisConfig < 0) maxTtlInMillisConfig = 0;
    deleteAfterAcknowledge = config.isDeleteAfterAcknowledge();
    consumerJobTimeout = config.getConsumerJobTimeoutInSeconds() * 1000L;
    consumerIdleTimeout = config.getConsumerIdleTimeoutInSeconds() * 1000L;
    trimScheduleDelay = Duration.ofSeconds(config.getCleanupCycleInSeconds());
    streamPrefix = config.getStreamPrefix();
    keyScanCycle = config.getKeyScanCycleInSeconds() * 1000L;
  }

  public void setRedisConnection(UniversalRedisConnection<String, ?> redisConnection) {
    this.redisConnection = redisConnection;
  }

  public void considerStream(String stream) {
    streams.put(stream, Boolean.TRUE);
  }

  public void trimStreamValues() {
    long now = System.currentTimeMillis();
    // scan existing zeebe streams and add them to the list of streams to be considered
    if (redisConnection != null && keyScanCycle > 0 && (now - lastKeyScan) > keyScanCycle) {
      lastKeyScan = now;
      try {
        getZeebeStreams().forEach(this::considerStream);
      } catch (Exception e) {
        logger.error("Error scanning for streams like " + streamPrefix + "*", e);
      }
    }
    // do the cleanup job
    if (redisConnection != null && streams.size() > 0 && acquireCleanupLock()) {
      try {
        // get ID according to max time to live
        final long maxTTLMillis = now - maxTtlInMillisConfig;
        final String maxTTLId = String.valueOf(maxTTLMillis);
        // get ID according to min time to live
        final long minTTLMillis = now - minTtlInMillisConfig;
        final String minTTLId = String.valueOf(minTTLMillis);
        logger.debug("trim streams {}", streams);
        // trim all streams
        List<String> keys = new ArrayList(streams.keySet());
        RedisStreamCommands<String, ?> streamCommands = redisConnection.syncStreamCommands();
        keys.forEach(
            stream -> {
              // 1. always trim stream according to maxTTL
              // side effect: prepares deleting too old pending messages in step 3
              if (maxTtlInMillisConfig > 0) {
                long numMaxTtlTrimmed =
                    streamCommands.xtrim(stream, new XTrimArgs().minId(maxTTLId));
                if (numMaxTtlTrimmed > 0) {
                  logger.debug("{}: {} cleaned records", stream, numMaxTtlTrimmed);
                }
              }
              // 2. get all consumer groups and add detailed consumer data
              var consumerGroups =
                  streamCommands.xinfoGroups(stream).stream()
                      .map(o -> XInfoGroup.fromXInfo(o, useProtoBuf))
                      .toList();
              for (XInfoGroup consumerGroup : consumerGroups) {
                consumerGroup.setConsumers(
                    streamCommands.xinfoConsumers(stream, consumerGroup.getName()).stream()
                        .map(o -> XInfoConsumer.fromXInfo(o, useProtoBuf))
                        .sorted(Comparator.comparingLong(XInfoConsumer::getIdle))
                        .toList());
              }
              // 3. resend abandoned messages
              // side effect: free too old pending messages according to max TTL
              if (consumerJobTimeout > 0) {
                consumerGroups.stream()
                    .filter(xi -> xi.getPending() > 0)
                    .forEach(
                        xi -> {
                          var consumerPending = Consumer.from(xi.getName(), "zeebe-pending");
                          var result =
                              streamCommands.xautoclaim(
                                  stream,
                                  new XAutoClaimArgs<String>()
                                      .consumer(consumerPending)
                                      .minIdleTime(consumerJobTimeout)
                                      .startId("0-0")
                                      .count(xi.getPending()));
                          var numResent = 0;
                          for (var message : result.getMessages()) {
                            var body = message.getBody();
                            var k = body.keySet().iterator().next();
                            if (Long.parseLong(k) >= maxTTLMillis) {
                              streamCommands.xadd(stream, k, body.get(k));
                              streamCommands.xdel(stream, message.getId());
                              numResent++;
                            }
                          }
                          streamCommands.xgroupDelconsumer(stream, consumerPending);
                          if (numResent > 0) {
                            logger.warn(
                                "Resent {} timed out pending messages for {}", numResent, stream);
                          }
                        });
              }
              // 4. trim according to last-delivered-id considering pending messages
              Optional<Long> minDelivered =
                  !deleteAfterAcknowledge
                      ? Optional.empty()
                      : consumerGroups.stream()
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
              }
              // 5. delete inactive consumers
              if (consumerIdleTimeout > 0) {
                var asyncStreamCommands = redisConnection.asyncStreamCommands();
                consumerGroups.forEach(
                    group -> {
                      var consumers = new ArrayList<>(group.getConsumers());
                      if (consumers.isEmpty()) return;
                      var youngestConsumer = consumers.remove(0);
                      consumers.stream()
                          .filter(
                              consumer ->
                                  consumer.getPending() == 0
                                      && consumer.getIdle()
                                          > youngestConsumer.getIdle() + consumerIdleTimeout)
                          .forEach(
                              consumer -> {
                                var delConsumer =
                                    Consumer.from(group.getName(), consumer.getName());
                                asyncStreamCommands.xgroupDelconsumer(stream, delConsumer);
                              });
                    });
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

  private List<String> getZeebeStreams() {
    Set<String> zeebeStreams = new HashSet<>();
    String cursor = "0";
    ScanArgs scanArgs = ScanArgs.Builder.matches(streamPrefix + "*").limit(100);
    var redisClusterCommands = redisConnection.syncClusterCommands();
    do {
      var result = redisClusterCommands.scan(ScanCursor.of(cursor), scanArgs);
      cursor = result.getCursor();
      zeebeStreams.addAll(result.getKeys());
    } while (!"0".equals(cursor));
    return zeebeStreams.stream()
        .filter(s -> !(CLEANUP_LOCK.equals(s) || CLEANUP_TIMESTAMP.equals(s)))
        .toList();
  }
}
