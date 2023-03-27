package io.zeebe.redis.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XTrimArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.exporter.proto.RecordTransformer;
import io.zeebe.exporter.proto.Schema;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class RedisExporter implements Exporter {

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

    logger.info("Starting exporter with configuration: {}", config);

    minTtlInMillisConfig = config.getMinTimeToLiveInSeconds() * 1000l;
    if (minTtlInMillisConfig < 0) minTtlInMillisConfig = 0;
    maxTtlInMillisConfig = config.getMaxTimeToLiveInSeconds() * 1000l;
    if (maxTtlInMillisConfig < 0) maxTtlInMillisConfig = 0;
    deleteAfterAcknowledge = config.isDeleteAfterAcknowledge();
    trimScheduleDelay = Duration.ofSeconds(config.getCleanupCycleInSeconds());
    streamPrefix = config.getName() + ":";

    final var filter = new RecordFilter(config);
    context.setFilter(filter);

    configureFormat();
  }

  private void configureFormat() {
    final var format = config.getFormat();
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
      final var now = System.currentTimeMillis();
      final String stream = streamPrefix.concat(record.getValueType().name());
      final var transformedRecord = recordTransformer.apply(record);
      final var messageId = redisConnection.async()
              .xadd(stream, Long.toString(now), transformedRecord);
      final var position = record.getPosition();
      messageId.thenRun(() -> {
        controller.updateLastExportedRecordPosition(position);
        streams.put(stream, Boolean.TRUE);
        try {
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
    if (streams.size() > 0) {
      // get ID according to max time to live
      final var maxTTLMillis = System.currentTimeMillis() - maxTtlInMillisConfig;
      final var maxTTLId = String.valueOf(maxTTLMillis);
      // get ID according to min time to live
      final var minTTLMillis = System.currentTimeMillis() - minTtlInMillisConfig;
      final var minTTLId = String.valueOf(minTTLMillis);
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
          var minDeliveredMillis = minDelivered.get();
          String xtrimMinId = String.valueOf(minDeliveredMillis);
          if (maxTtlInMillisConfig > 0 && maxTTLMillis > minDeliveredMillis) {
            xtrimMinId = maxTTLId;
          } else if (minTtlInMillisConfig > 0 && minTTLMillis < minDeliveredMillis){
            xtrimMinId = minTTLId;
           }
          redisConnection.async().xtrim(stream, new XTrimArgs().minId(xtrimMinId));
        } else if (maxTtlInMillisConfig > 0) {
          redisConnection.async().xtrim(stream, new XTrimArgs().minId(maxTTLId));
        }
      });
    }
    controller.scheduleCancellableTask(trimScheduleDelay, this::trimStreamValues);
  }

}
