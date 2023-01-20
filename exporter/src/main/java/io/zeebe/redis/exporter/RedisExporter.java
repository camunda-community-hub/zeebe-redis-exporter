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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private long ttlInMillis = 0;

  private Duration trimScheduleDelay;

  private String streamPrefix;

  @Override
  public void configure(Context context) {
    logger = context.getLogger();
    config = context.getConfiguration().instantiate(ExporterConfiguration.class);

    logger.debug("Starting exporter with configuration: {}", config);

    ttlInMillis = config.getTimeToLiveInSeconds() * 1000l;
    trimScheduleDelay = Duration.ofSeconds(Math.min(60, config.getTimeToLiveInSeconds()));
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

    if (config.getTimeToLiveInSeconds() > 0) {
      controller.scheduleCancellableTask(trimScheduleDelay, this::timeOutStreamValues);
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
      messageId.thenRun(() -> {
        streams.put(stream, Boolean.TRUE);
        logger.trace("Added a record with key {} to stream {}, messageId: {}", now, stream, messageId);
      });
    }
    controller.updateLastExportedRecordPosition(record.getPosition());
  }

  private byte[] recordToProtobuf(Record record) {
    final Schema.Record dto = RecordTransformer.toGenericRecord(record);
    return dto.toByteArray();
  }

  private String recordToJson(Record record) {
    return record.toJson();
  }

  private void timeOutStreamValues() {
    if (streams.size() > 0) {
      final var minId = String.valueOf(System.currentTimeMillis() - ttlInMillis);
      logger.debug("trim streams {} with minId={}", streams, minId);
      List<String> keys = new ArrayList(streams.keySet());
      keys.forEach(stream -> {
        redisConnection.async().xtrim(stream, new XTrimArgs().minId(minId));
      });
    }
    controller.scheduleCancellableTask(trimScheduleDelay, this::timeOutStreamValues);
  }
}
