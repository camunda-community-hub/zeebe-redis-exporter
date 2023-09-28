package io.zeebe.redis.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.exporter.proto.RecordTransformer;
import io.zeebe.exporter.proto.Schema;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class RedisExporter implements Exporter {

  private final static String CLEANUP_LOCK = "zeebe:cleanup-lock";
  private final static String CLEANUP_TIMESTAMP = "zeebe:cleanup-time";

  private ExporterConfiguration config;
  private Logger logger;
  private Controller controller;

  private RedisClient redisClient;
  private StatefulRedisConnection<String, ?> redisConnection;
  private Function<Record, ?> recordTransformer;

  private Map<String, Boolean> streams = new ConcurrentHashMap<>();

  private boolean useProtoBuf = false;
  private long maxTtlInMillisConfig = 0;
  private long minTtlInMillisConfig = 0;
  private boolean deleteAfterAcknowledge = false;

  private Duration trimScheduleDelay;

  private String streamPrefix;

  private HashMap<String, Long> positions = new HashMap<>();

  @Override
  public void configure(Context context) {
    logger = context.getLogger();
    config = context.getConfiguration().instantiate(ExporterConfiguration.class);

    logger.info("Starting Redis exporter with configuration: {}", config);

    minTtlInMillisConfig = config.getMinTimeToLiveInSeconds() * 1000l;
    if (minTtlInMillisConfig < 0) minTtlInMillisConfig = 0;
    maxTtlInMillisConfig = config.getMaxTimeToLiveInSeconds() * 1000l;
    if (maxTtlInMillisConfig < 0) maxTtlInMillisConfig = 0;
    deleteAfterAcknowledge = config.isDeleteAfterAcknowledge();
    trimScheduleDelay = Duration.ofSeconds(config.getCleanupCycleInSeconds());
    streamPrefix = config.getName() + ":";

    final RecordFilter filter = new RecordFilter(config);
    context.setFilter(filter);

    configureFormat();
  }

  private void configureFormat() {
    final String format = config.getFormat();
    if (format.equalsIgnoreCase("protobuf")) {
      recordTransformer = this::recordToProtobuf;
      useProtoBuf = true;
    } else if (format.equalsIgnoreCase("json")) {
      recordTransformer = this::recordToJson;

    } else {
      throw new IllegalArgumentException(
          String.format(
              "Expected the parameter 'format' to be one of 'protobuf' or 'json' but was '%s'",
              format));
    }
  }

  @Override
  public void open(Controller controller) {
    this.controller = controller;

    if (config.getRemoteAddress().isEmpty()) {
      throw new IllegalStateException("Missing ZEEBE_REDIS_REMOTE_ADDRESS configuration.");
    }

    redisClient = RedisClient.create(config.getRemoteAddress().get());
    redisConnection = useProtoBuf ? redisClient.connect(new ProtobufCodec()) : redisClient.connect();

    logger.info("Successfully connected Redis exporter to {}", config.getRemoteAddress().get());

    if (config.getCleanupCycleInSeconds() > 0 &&
            (config.isDeleteAfterAcknowledge() || config.getMaxTimeToLiveInSeconds() > 0)) {
      controller.scheduleCancellableTask(trimScheduleDelay, this::trimStreamValues);
    }
  }

  @Override
  public void close() {
    if (redisConnection != null) {
      redisConnection.close();
      redisConnection = null;
    }
    redisClient.shutdown();
  }

  @Override
  public void export(Record record) {
    if (redisConnection != null) {
      final long now = System.currentTimeMillis();
      final String stream = streamPrefix.concat(record.getValueType().name());
      final Object transformedRecord = recordTransformer.apply(record);
      final RedisFuture<String> messageId = redisConnection.async()
              .xadd(stream, Long.toString(now), transformedRecord);
      final long position = record.getPosition();
      messageId.thenRun(() -> {
        controller.updateLastExportedRecordPosition(position);
        streams.put(stream, Boolean.TRUE);
        try {
          if (logger.isTraceEnabled())
            logger.trace("Added a record with key {} to stream {}, messageId: {}", now, stream, messageId.get());
        } catch (Exception ex) {
          // NOOP: required catch from messageId.get()
        }
      });
    }
  }

  private byte[] recordToProtobuf(Record record) {
    final Schema.Record dto = RecordTransformer.toGenericRecord(record);
    return dto.toByteArray();
  }

  private String recordToJson(Record record) {
    return record.toJson();
  }

  private void trimStreamValues() {
    if (streams.size() > 0 && acquireCleanupLock()) {
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
        logger.error("Error during cleanup due to possible Redis unavailability: ", ex.getMessage());
      } catch (Exception ex) {
        logger.error("Error during cleanup", ex);
      } finally {
        releaseCleanupLock();
      }
    }
    controller.scheduleCancellableTask(trimScheduleDelay, this::trimStreamValues);
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
      logger.error("Error acquiring cleanup lock due to possible Redis unavailability: ", ex.getMessage());
    } catch (Exception ex) {
      logger.error("Error acquiring cleanup lock", ex);
    }
    return false;
  }

  private void releaseCleanupLock() {
    try {
      redisConnection.sync().del(CLEANUP_LOCK);
    } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
      logger.error("Error releasing cleanup lock due to possible Redis unavailability: ", ex.getMessage());
    } catch (Exception ex) {
      logger.error("Error releasing cleanup lock", ex);
    }
  }
}
