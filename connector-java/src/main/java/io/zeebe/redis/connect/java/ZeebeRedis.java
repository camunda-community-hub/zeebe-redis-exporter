package io.zeebe.redis.connect.java;

import com.google.protobuf.InvalidProtocolBufferException;
import io.camunda.zeebe.protocol.record.ValueType;
import io.lettuce.core.*;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.BoundedAsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.zeebe.exporter.proto.Schema;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZeebeRedis implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZeebeRedis.class);

  private static final Map<String, Class<? extends com.google.protobuf.Message>>
      RECORD_MESSAGE_TYPES;

  static {
    RECORD_MESSAGE_TYPES =
        Map.ofEntries(
            typeEntry(ValueType.AUTHORIZATION.name(), Schema.AuthorizationRecord.class),
            typeEntry(ValueType.CLOCK.name(), Schema.ClockRecord.class),
            typeEntry(
                ValueType.COMPENSATION_SUBSCRIPTION.name(),
                Schema.CompensationSubscriptionRecord.class),
            typeEntry(ValueType.DEPLOYMENT.name(), Schema.DeploymentRecord.class),
            typeEntry(
                ValueType.DEPLOYMENT_DISTRIBUTION.name(),
                Schema.DeploymentDistributionRecord.class),
            typeEntry(ValueType.ERROR.name(), Schema.ErrorRecord.class),
            typeEntry(ValueType.ESCALATION.name(), Schema.EscalationRecord.class),
            typeEntry(ValueType.FORM.name(), Schema.FormRecord.class),
            typeEntry(ValueType.INCIDENT.name(), Schema.IncidentRecord.class),
            typeEntry(ValueType.JOB.name(), Schema.JobRecord.class),
            typeEntry(ValueType.JOB_BATCH.name(), Schema.JobBatchRecord.class),
            typeEntry(ValueType.MESSAGE_BATCH.name(), Schema.MessageBatchRecord.class),
            typeEntry(ValueType.MESSAGE_CORRELATION.name(), Schema.MessageCorrelationRecord.class),
            typeEntry(
                ValueType.MESSAGE_START_EVENT_SUBSCRIPTION.name(),
                Schema.MessageStartEventSubscriptionRecord.class),
            typeEntry(
                ValueType.MESSAGE_SUBSCRIPTION.name(), Schema.MessageSubscriptionRecord.class),
            typeEntry(ValueType.MESSAGE.name(), Schema.MessageRecord.class),
            typeEntry(ValueType.PROCESS.name(), Schema.ProcessRecord.class),
            typeEntry(ValueType.PROCESS_EVENT.name(), Schema.ProcessEventRecord.class),
            typeEntry(ValueType.PROCESS_INSTANCE.name(), Schema.ProcessInstanceRecord.class),
            typeEntry(
                ValueType.PROCESS_INSTANCE_BATCH.name(), Schema.ProcessInstanceBatchRecord.class),
            typeEntry(
                ValueType.PROCESS_INSTANCE_CREATION.name(),
                Schema.ProcessInstanceCreationRecord.class),
            typeEntry(
                ValueType.PROCESS_INSTANCE_MIGRATION.name(),
                Schema.ProcessInstanceMigrationRecord.class),
            typeEntry(
                ValueType.PROCESS_INSTANCE_RESULT.name(), Schema.ProcessInstanceResultRecord.class),
            typeEntry(
                ValueType.PROCESS_MESSAGE_SUBSCRIPTION.name(),
                Schema.ProcessMessageSubscriptionRecord.class),
            typeEntry(ValueType.RESOURCE.name(), Schema.ResourceRecord.class),
            typeEntry(ValueType.RESOURCE_DELETION.name(), Schema.ResourceDeletionRecord.class),
            typeEntry(ValueType.TIMER.name(), Schema.TimerRecord.class),
            typeEntry(ValueType.USER.name(), Schema.UserRecord.class),
            typeEntry(ValueType.USER_TASK.name(), Schema.UserTaskRecord.class),
            typeEntry(ValueType.VARIABLE.name(), Schema.VariableRecord.class),
            typeEntry(ValueType.VARIABLE_DOCUMENT.name(), Schema.VariableDocumentRecord.class));
  }

  private static AbstractMap.SimpleEntry<String, Class<? extends com.google.protobuf.Message>>
      typeEntry(String valueType, Class<? extends com.google.protobuf.Message> messageClass) {
    return new AbstractMap.SimpleEntry<>(valueType, messageClass);
  }

  private UniversalRedisClient redisClient;
  private UniversalRedisConnection<String, byte[]> redisConnection;
  private BoundedAsyncPool<StatefulRedisClusterConnection<String, byte[]>> redisPool;

  private int xreadBlockMillis;

  private int xreadCount;

  private String consumerGroup;

  private String consumerId;

  private String prefix;

  private XReadArgs.StreamOffset[] offsets;

  private final Map<String, List<Consumer<?>>> listeners;

  private boolean deleteMessages;

  private Future<?> readFuture;
  private ExecutorService executorService;

  private boolean reconnectUsesNewConnection;
  private long reconnectIntervalMillis;
  private Future<?> reconnectFuture;
  private ExecutorService reconnectExecutorService;
  private volatile boolean isClosed = false;
  private volatile boolean forcedClose = false;

  private boolean shouldDestroyConsumerGroupOnClose;
  private boolean shouldDeleteConsumerIdOnClose;

  protected ZeebeRedis(
      UniversalRedisClient redisClient,
      UniversalRedisConnection<String, byte[]> redisConnection,
      boolean reconnectUsesNewConnection,
      Duration reconnectInterval,
      int xreadBlockMillis,
      int xreadCount,
      String consumerGroup,
      String consumerId,
      String prefix,
      XReadArgs.StreamOffset<String>[] offsets,
      Map<String, List<Consumer<?>>> listeners,
      boolean deleteMessages,
      boolean shouldDestroyConsumerGroupOnClose,
      boolean shouldDeleteConsumerIdOnClose) {
    this.redisClient = redisClient;
    this.redisConnection = redisConnection;
    this.reconnectUsesNewConnection = reconnectUsesNewConnection;
    this.reconnectIntervalMillis = reconnectInterval.toMillis();
    this.xreadBlockMillis = xreadBlockMillis;
    this.xreadCount = xreadCount;
    this.consumerGroup = consumerGroup;
    this.consumerId = consumerId;
    this.prefix = prefix;
    this.offsets = offsets;
    this.listeners = listeners;
    this.deleteMessages = deleteMessages;
    this.shouldDestroyConsumerGroupOnClose = shouldDestroyConsumerGroupOnClose;
    if (this.shouldDestroyConsumerGroupOnClose) {
      LOGGER.warn(
          "No Redis consumer group configured! Will use unique disposable group {}", consumerGroup);
    }
    this.shouldDeleteConsumerIdOnClose = shouldDeleteConsumerIdOnClose;
  }

  /** Returns a new builder to read from the Redis Streams. */
  public static RedisConnectionBuilder newBuilder(RedisClient redisClient) {
    return new RedisConnectionBuilder(redisClient);
  }

  public static RedisConnectionBuilder newBuilder(RedisClusterClient redisClient) {
    return new RedisConnectionBuilder(redisClient);
  }

  protected void start() {
    if (redisClient.isCluster()) {
      CompletionStage<BoundedAsyncPool<StatefulRedisClusterConnection<String, byte[]>>> poolFuture =
          AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
              () -> redisClient.getRedisClusterClient().connectAsync(new ProtobufCodec()),
              BoundedPoolConfig.builder().maxTotal(offsets.length).build());
      try {
        redisPool = poolFuture.toCompletableFuture().get();
      } catch (InterruptedException e) {
        LOGGER.error("Error creating Redis cluster connection pool", e);
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        LOGGER.error("Error creating Redis cluster connection pool", e);
        throw new RuntimeException(e);
      }
      if (!shouldDestroyConsumerGroupOnClose) {
        // the connection has been used to create consumer groups and can now be closed
        // because reading from streams is done by the connection pool.
        redisConnection.close();
      }
      if (reconnectUsesNewConnection) {
        // take care to not close the whole connection pool in case of failing cluster connections
        LOGGER.warn(
            "Parameter 'reconnectUsesNewConnection' has no effect when using RedisClusterClient.");
        reconnectUsesNewConnection = false;
      }
    } else {
      // if we're not connected to a cluster we eventually handle reconnects ourselves
      redisConnection.addListener(
          new RedisConnectionStateAdapter() {
            public void onRedisConnected(
                RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
              LOGGER.info("Redis reconnected.");
            }

            public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
              if (forcedClose) return;
              LOGGER.warn("Redis connection lost.");
              if (reconnectUsesNewConnection) {
                doClose();
                reconnectExecutorService = Executors.newSingleThreadExecutor();
                reconnectFuture = reconnectExecutorService.submit(ZeebeRedis.this::reconnect);
              }
            }
          });
    }
    forcedClose = false;
    isClosed = false;
    executorService = Executors.newSingleThreadExecutor();
    readFuture = executorService.submit(this::readFromStream);
  }

  public void reconnect() {
    ProtobufCodec protobufCodec = new ProtobufCodec();
    do {
      try {
        Thread.sleep(reconnectIntervalMillis);
        redisConnection = redisClient.connect(protobufCodec);
        LOGGER.info("Redis reconnected.");
        var syncStreamCommands = redisConnection.syncStreamCommands();
        listeners.keySet().stream()
            .forEach(
                stream -> {
                  try {
                    syncStreamCommands.xgroupCreate(
                        XReadArgs.StreamOffset.from(stream, "0-0"),
                        consumerGroup,
                        XGroupCreateArgs.Builder.mkstream());
                  } catch (RedisBusyException ex) {
                    // NOOP: consumer group already exists
                  }
                });
        start();
        return;
      } catch (InterruptedException ex) {
        return;
      } catch (Exception ex) {
        LOGGER.trace("Redis reconnect failure: {}", ex.getMessage());
      }
    } while (true);
  }

  public boolean isClosed() {
    return isClosed;
  }

  /** Stop reading from the Redis Streams. */
  @Override
  public void close() {
    isClosed = true;
    if (shouldDestroyConsumerGroupOnClose) {
      var syncStreamCommands = redisConnection.syncStreamCommands();
      Arrays.stream(offsets)
          .forEach(
              o -> {
                String stream = String.valueOf(o.getName());
                LOGGER.trace("Destroying consumer group {} of stream {}", consumerGroup, stream);
                try {
                  syncStreamCommands.xgroupDestroy(stream, consumerGroup);
                } catch (Exception ex) {
                  LOGGER.error(
                      "Error destroying consumer group {} of stream {}", consumerGroup, stream);
                }
              });
    } else if (shouldDeleteConsumerIdOnClose) {
      var syncStreamCommands = redisConnection.syncStreamCommands();
      Arrays.stream(offsets)
          .forEach(
              o -> {
                String stream = String.valueOf(o.getName());
                LOGGER.trace(
                    "Deleting consumer {} of consumer group {} for stream {}",
                    consumerId,
                    consumerGroup,
                    stream);
                try {
                  syncStreamCommands.xgroupDelconsumer(
                      stream, io.lettuce.core.Consumer.from(consumerGroup, consumerId));
                } catch (Exception ex) {
                  LOGGER.error(
                      "Error deleting consumer {} of consumer group {} for stream {}",
                      consumerId,
                      consumerGroup,
                      stream);
                }
              });
    }
    forceClose();
  }

  private void forceClose() {
    forcedClose = true;
    doClose();
  }

  private void doClose() {
    LOGGER.info(
        "Closing Consumer[group={}, id={}]. Stop reading from streams '{}*'.",
        consumerGroup,
        consumerId,
        prefix);

    isClosed = true;

    if (redisPool != null) {
      redisPool.closeAsync();
      redisPool = null;
    }
    if (readFuture != null) {
      readFuture.cancel(true);
      readFuture = null;
    }
    if (executorService != null) {
      executorService.shutdown();
      executorService = null;
    }
    if (reconnectFuture != null) {
      reconnectFuture.cancel(true);
      reconnectFuture = null;
    }
    if (reconnectExecutorService != null) {
      reconnectExecutorService.shutdown();
      reconnectExecutorService = null;
    }
    redisConnection.close();
  }

  private void readFromStream() {
    if (reconnectFuture != null) {
      reconnectFuture.cancel(true);
      reconnectFuture = null;
    }
    if (reconnectExecutorService != null) {
      reconnectExecutorService.shutdown();
      reconnectExecutorService = null;
    }
    if (!redisClient.isCluster()) {
      while (!isClosed) {
        readNext(redisConnection, offsets);
      }
    } else {
      for (XReadArgs.StreamOffset offset : offsets) {
        redisPool
            .acquire()
            .thenAcceptAsync(
                connection -> {
                  var universalRedisConnection = new UniversalRedisConnection(connection);
                  while (!isClosed) {
                    readNext(universalRedisConnection, offset);
                  }
                  redisPool.release(connection);
                });
      }
    }
  }

  private void readNext(
      UniversalRedisConnection redisConnection, XReadArgs.StreamOffset... offsets) {
    if (offsets.length == 1) {
      LOGGER.trace("Consumer[id={}] reads from stream '{}'", consumerId, offsets[0].getName());
    } else {
      LOGGER.trace("Consumer[id={}] reads from streams '{}*'", consumerId, prefix);
    }

    try {
      var asyncStreamCommands = redisConnection.asyncStreamCommands();
      List<StreamMessage<String, byte[]>> messages =
          (List<StreamMessage<String, byte[]>>)
              asyncStreamCommands
                  .xreadgroup(
                      io.lettuce.core.Consumer.from(consumerGroup, consumerId),
                      XReadArgs.Builder.block(xreadBlockMillis).count(xreadCount),
                      offsets)
                  .get();

      for (StreamMessage<String, byte[]> message : messages) {
        LOGGER.trace(
            "Consumer[id={}] received message {} from {}",
            consumerId,
            message.getId(),
            message.getStream());
        var success = handleRecord(message);
        asyncStreamCommands.xack(message.getStream(), consumerGroup, message.getId());
        if (deleteMessages && success) {
          asyncStreamCommands.xdel(message.getStream(), message.getId());
        }
      }
    } catch (IllegalArgumentException ex) {
      // should not happen with a correct configuration
      redisConnection.releaseFromPool(redisPool);
      LOGGER.error("Illegal arguments for xreadgroup: {}. Closing Redis client.", ex.getMessage());
      try {
        forceClose();
      } catch (Exception closingFailure) {
        LOGGER.debug("Failure while closing the client", closingFailure);
      }
    } catch (RedisCommandTimeoutException e) {
      if (!isClosed) {
        LOGGER.debug(
            "Consumer[group={}, id={}] timed out reading from streams '{}*'",
            consumerGroup,
            consumerId,
            prefix);
      }
    } catch (RedisCommandExecutionException e) {
      // should not happen, but we want to recover anyway
      redisConnection.releaseFromPool(redisPool);
      if (!isClosed) {
        LOGGER.error(
            "Consumer[group={}, id={}] failed to read from streams '{}*': {}. Initiating reconnect.",
            consumerGroup,
            consumerId,
            prefix,
            e.getMessage());
        try {
          forceClose();
        } catch (Exception closingFailure) {
          LOGGER.debug("Failure while closing the client", closingFailure);
        }
        reconnectExecutorService = Executors.newSingleThreadExecutor();
        reconnectFuture = reconnectExecutorService.submit(ZeebeRedis.this::reconnect);
      }
    } catch (Exception e) {
      // TODO: should not happen, should we recover like above?
      if (!isClosed) {
        LOGGER.error(
            "Consumer[group={}, id={}] failed to read from streams '{}*'",
            consumerGroup,
            consumerId,
            prefix,
            e);
      }
    }
  }

  private boolean handleRecord(StreamMessage<String, byte[]> message)
      throws InvalidProtocolBufferException {
    final var messageValue = message.getBody().values().iterator().next();
    final var genericRecord = Schema.Record.parseFrom(messageValue);
    final var recordType = RECORD_MESSAGE_TYPES.get(message.getStream().substring(prefix.length()));

    try {
      handleRecord(message.getStream(), genericRecord, recordType);
      return true;
    } catch (InvalidProtocolBufferException e) {
      LOGGER.error(
          "Failed to deserialize Protobuf message {} from {}",
          message.getId(),
          message.getStream(),
          e);
      return true; // not interested in reading corrupt data again
    } catch (Exception ex) {
      LOGGER.error(
          "Error handling message {} from {}: {}",
          message.getId(),
          message.getStream(),
          ex.getMessage());
      return false;
    }
  }

  private <T extends com.google.protobuf.Message> void handleRecord(
      String stream, Schema.Record genericRecord, Class<T> t)
      throws InvalidProtocolBufferException {
    final var record = genericRecord.getRecord().unpack(t);
    LOGGER.trace("Consumer[id={}] handling record {}", consumerId, record);

    listeners
        .getOrDefault(stream, List.of())
        .forEach(listener -> ((Consumer<T>) listener).accept(record));
  }
}
