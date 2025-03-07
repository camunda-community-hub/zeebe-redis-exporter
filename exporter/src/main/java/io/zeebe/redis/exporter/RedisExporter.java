package io.zeebe.redis.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.resource.ClientResources;
import io.zeebe.exporter.proto.RecordTransformer;
import io.zeebe.exporter.proto.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RedisExporter implements Exporter {

  private ExporterConfiguration config;
  private Logger logger;

  private UniversalRedisClient redisClient;
  private UniversalRedisConnection<String, ?> cleanupConnection;
  private UniversalRedisConnection<String, ?> senderConnection;
  private Function<Record, ?> recordTransformer;

  private boolean useProtoBuf = false;

  private String streamPrefix;

  private EventQueue eventQueue = new EventQueue();

  private RedisCleaner redisCleaner;

  private RedisSender redisSender;

  private Controller controller;

  // The ExecutorService allows to schedule a regular task independent of the actual load
  // which controller.scheduleCancellableTask sadly didn't do.
  private ScheduledExecutorService senderThread = Executors.newSingleThreadScheduledExecutor();

  private ScheduledExecutorService cleanerThread = Executors.newSingleThreadScheduledExecutor();

  // Startup handling in case of Redis connection failure
  private ScheduledExecutorService startupThread;
  private boolean fullyLoggedStartupException = false;
  private List<Integer> reconnectIntervals = new ArrayList<>(List.of(2,3,3,4,4,4,5));

  @Override
  public void configure(Context context) {
    logger = context.getLogger();
    config = context.getConfiguration().instantiate(ExporterConfiguration.class);

    logger.info("Starting Redis exporter version {} with configuration: {}",
            this.getClass().getPackage().getImplementationVersion(), config);

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
      logger.error("Redis configuration error: Missing remote address. Please check ZEEBE_REDIS_REMOTE_ADDRESS environment variable or configuration.");
      throw new IllegalStateException("Missing ZEEBE_REDIS_REMOTE_ADDRESS configuration.");
    }
    this.controller = controller;

    logger.info("Initializing Redis client with configuration: useClusterClient={}, ioThreadPoolSize={}, remoteAddress={}",
            config.isUseClusterClient(), config.getIoThreadPoolSize(), config.getRemoteAddress().get());

    if (config.isUseClusterClient()) {
        logger.debug("Creating Redis cluster client...");
        var clusterClient = RedisClusterClient.create(
                ClusterClientSettings.createResourcesFromConfig(config), config.getRemoteAddress().get());
        clusterClient.setOptions(ClusterClientSettings.createStandardOptions());
        redisClient = new UniversalRedisClient(clusterClient);
    } else {
        logger.debug("Creating Redis standalone client...");
        redisClient = new UniversalRedisClient(RedisClient.create(
                ClientResources.builder().ioThreadPoolSize(config.getIoThreadPoolSize()).build(),
                config.getRemoteAddress().get()));
    }
    connectToRedis();
  }

  private void connectToRedis() {
    boolean failure = false;
    // try to connect
    try {
      logger.info("Attempting to establish Redis connection to {}", config.getRemoteAddress().get());
      senderConnection = useProtoBuf ? redisClient.connect(new ProtobufCodec()) : redisClient.connect();
      cleanupConnection = useProtoBuf ? redisClient.connect(new ProtobufCodec()) : redisClient.connect();
      logger.info("Successfully connected Redis exporter to {} using {} format", 
              config.getRemoteAddress().get(), 
              useProtoBuf ? "protobuf" : "json");
    } catch (RedisConnectionException ex) {
      if (!fullyLoggedStartupException) {
        logger.error("Redis connection error: Failed to connect to {}. Error details: {}", 
                config.getRemoteAddress().get(), ex.getMessage(), ex);
        logger.error("Current Redis configuration: useClusterClient={}, ioThreadPoolSize={}, format={}", 
                config.isUseClusterClient(), config.getIoThreadPoolSize(), 
                useProtoBuf ? "protobuf" : "json");
        fullyLoggedStartupException = true;
      } else {
        logger.warn("Redis connection retry failed for {}: {}", 
                config.getRemoteAddress().get(), ex.getMessage());
      }
      failure = true;
    }

    // upon successful connection initialize the sender
    if (redisSender == null && senderConnection != null) {
      logger.debug("Initializing Redis sender with batch size {} and cycle {} ms", 
              config.getBatchSize(), config.getBatchCycleMillis());
      redisSender = new RedisSender(config, controller, senderConnection, logger);
      senderThread.schedule(this::sendBatches, config.getBatchCycleMillis(), TimeUnit.MILLISECONDS);
    }

    // always initialize the cleaner
    if (redisCleaner == null) {
      logger.debug("Initializing Redis cleaner with cleanup cycle {} seconds", 
              config.getCleanupCycleInSeconds());
      redisCleaner = new RedisCleaner(cleanupConnection, useProtoBuf, config, logger);
      if (config.getCleanupCycleInSeconds() > 0 &&
              (config.isDeleteAfterAcknowledge() || config.getMaxTimeToLiveInSeconds() > 0)) {
        cleanerThread.schedule(this::trimStreamValues, config.getCleanupCycleInSeconds(), TimeUnit.SECONDS);
      }
    // upon late successful connection propagate it to cleaner
    } else if (cleanupConnection != null) {
      logger.debug("Updating Redis cleaner with new connection");
      redisCleaner.setRedisConnection(cleanupConnection);
    }

    // if initial connection has failed, try again later
    if (failure) {
      if (startupThread == null) {
        startupThread = Executors.newSingleThreadScheduledExecutor();
      }
      int delay = reconnectIntervals.size() > 1 ? reconnectIntervals.remove(0) : reconnectIntervals.get(0);
      logger.info("Scheduling Redis connection retry in {} seconds", delay);
      startupThread.schedule(this::connectToRedis, delay, TimeUnit.SECONDS);
    } else if (startupThread != null ) {
      logger.info("Connection successful, shutting down retry mechanism");
      startupThread.shutdown();
      startupThread = null;
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
    if (redisClient != null) {
      redisClient.shutdown();
    }
    if (startupThread != null) {
      startupThread.shutdown();
      startupThread = null;
    }
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
