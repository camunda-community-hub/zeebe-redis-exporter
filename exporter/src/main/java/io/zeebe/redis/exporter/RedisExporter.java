package io.zeebe.redis.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.ClientResources;
import io.zeebe.exporter.proto.RecordTransformer;
import io.zeebe.exporter.proto.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RedisExporter implements Exporter {

  private ExporterConfiguration config;
  private Logger logger;

  private RedisClient redisClient;
  private StatefulRedisConnection<String, ?> cleanupConnection;
  private StatefulRedisConnection<String, ?> senderConnection;
  private Function<Record, ?> recordTransformer;

  private boolean useProtoBuf = false;

  private String streamPrefix;

  private EventQueue eventQueue = new EventQueue();

  private RedisCleaner redisCleaner;

  private RedisSender redisSender;

  // The ExecutorService allows to schedule a regular task independent of the actual load
  // which controller.scheduleCancellableTask sadly didn't do.
  private ScheduledExecutorService senderThread = Executors.newSingleThreadScheduledExecutor();

  private ScheduledExecutorService cleanerThread = Executors.newSingleThreadScheduledExecutor();

  @Override
  public void configure(Context context) {
    logger = context.getLogger();
    config = context.getConfiguration().instantiate(ExporterConfiguration.class);

    logger.info("Starting Redis exporter with configuration: {}", config);

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
    if (config.getRemoteAddress().isEmpty()) {
      throw new IllegalStateException("Missing ZEEBE_REDIS_REMOTE_ADDRESS configuration.");
    }

    redisClient = RedisClient.create(
            ClientResources.builder().ioThreadPoolSize(config.getIoThreadPoolSize()).build(),
            config.getRemoteAddress().get());
    senderConnection = useProtoBuf ? redisClient.connect(new ProtobufCodec()) : redisClient.connect();
    cleanupConnection = useProtoBuf ? redisClient.connect(new ProtobufCodec()) : redisClient.connect();
    logger.info("Successfully connected Redis exporter to {}", config.getRemoteAddress().get());

    redisSender = new RedisSender(config, controller, senderConnection, logger);
    senderThread.schedule(this::sendBatches, config.getBatchCycleMillis(), TimeUnit.MILLISECONDS);

    redisCleaner = new RedisCleaner(cleanupConnection, useProtoBuf, config, logger);
    if (config.getCleanupCycleInSeconds() > 0 &&
            (config.isDeleteAfterAcknowledge() || config.getMaxTimeToLiveInSeconds() > 0)) {
      cleanerThread.schedule(this::trimStreamValues, config.getCleanupCycleInSeconds(), TimeUnit.SECONDS);
    }
  }

  @Override
  public void close() {
    senderThread.shutdown();
    if (senderConnection != null) {
      senderConnection.close();
      senderConnection = null;
    }
    cleanerThread.shutdown();
    if (cleanupConnection != null) {
      cleanupConnection.close();
      cleanupConnection = null;
    }
    redisClient.shutdown();
  }

  @Override
  public void export(Record record) {
    final String stream = streamPrefix.concat(record.getValueType().name());
    final RedisEvent redisEvent = new RedisEvent(stream,
            System.currentTimeMillis(), recordTransformer.apply(record));
    eventQueue.addEvent(new ImmutablePair<>(record.getPosition(), redisEvent));
    redisCleaner.considerStream(stream);
  }

  private void sendBatches() {
    redisSender.sendFrom(eventQueue);
    senderThread.schedule(this::sendBatches, config.getBatchCycleMillis(), TimeUnit.MILLISECONDS);
  }


  private byte[] recordToProtobuf(Record record) {
    final Schema.Record dto = RecordTransformer.toGenericRecord(record);
    return dto.toByteArray();
  }

  private String recordToJson(Record record) {
    return record.toJson();
  }

  private void trimStreamValues() {
    redisCleaner.trimStreamValues();
    cleanerThread.schedule(this::trimStreamValues, config.getCleanupCycleInSeconds(), TimeUnit.SECONDS);
  }
}
