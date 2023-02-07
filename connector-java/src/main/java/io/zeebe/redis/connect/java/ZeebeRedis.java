package io.zeebe.redis.connect.java;

import com.google.protobuf.InvalidProtocolBufferException;
import io.camunda.zeebe.protocol.record.ValueType;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.exporter.proto.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class ZeebeRedis implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZeebeRedis.class);

  private static final Map <String, Class<? extends com.google.protobuf.Message>> RECORD_MESSAGE_TYPES;

  static {
    RECORD_MESSAGE_TYPES = Map.ofEntries(
            typeEntry(ValueType.DEPLOYMENT.name(), Schema.DeploymentRecord.class),
            typeEntry(ValueType.DEPLOYMENT_DISTRIBUTION.name(), Schema.DeploymentDistributionRecord.class),
            typeEntry(ValueType.ERROR.name(), Schema.ErrorRecord.class),
            typeEntry(ValueType.INCIDENT.name(), Schema.IncidentRecord.class),
            typeEntry(ValueType.JOB.name(), Schema.JobRecord.class),
            typeEntry(ValueType.JOB_BATCH.name(), Schema.JobBatchRecord.class),
            typeEntry(ValueType.MESSAGE_START_EVENT_SUBSCRIPTION.name(), Schema.MessageStartEventSubscriptionRecord.class),
            typeEntry(ValueType.MESSAGE_SUBSCRIPTION.name(), Schema.MessageSubscriptionRecord.class),
            typeEntry(ValueType.MESSAGE.name(), Schema.MessageRecord.class),
            typeEntry(ValueType.PROCESS.name(), Schema.ProcessRecord.class),
            typeEntry(ValueType.PROCESS_EVENT.name(), Schema.ProcessEventRecord.class),
            typeEntry(ValueType.PROCESS_INSTANCE.name(), Schema.ProcessInstanceRecord.class),
            typeEntry(ValueType.PROCESS_INSTANCE_CREATION.name(), Schema.ProcessInstanceCreationRecord.class),
            typeEntry(ValueType.PROCESS_MESSAGE_SUBSCRIPTION.name(), Schema.ProcessMessageSubscriptionRecord.class),
            typeEntry(ValueType.TIMER.name(), Schema.TimerRecord.class),
            typeEntry(ValueType.VARIABLE.name(), Schema.VariableRecord.class),
            typeEntry(ValueType.VARIABLE_DOCUMENT.name(), Schema.VariableDocumentRecord.class));
  }
  private static AbstractMap.SimpleEntry<String, Class<? extends com.google.protobuf.Message>> typeEntry(
          String valueType, Class<? extends com.google.protobuf.Message> messageClass) {
    return new AbstractMap.SimpleEntry<>(valueType, messageClass);
  }

  private RedisClient redisClient;

  private StatefulRedisConnection<String, byte[]> redisConnection;

  private String consumerGroup;

  private String consumerId;

  private String prefix;

  private XReadArgs.StreamOffset[] offsets;

  private final Map<String, List<Consumer<?>>> listeners;

  private boolean deleteMessages = false;

  private Future<?> future;
  private ExecutorService executorService;

  private boolean reconnectUsesNewConnection = false;
  private long reconnectIntervalMillis;
  private Future<?> reconnectFuture;
  private ExecutorService reconnectExecutorService;
  private volatile boolean isClosed = false;
  private volatile boolean externalClose = false;

  private ZeebeRedis(RedisClient redisClient,
                     StatefulRedisConnection<String, byte[]> redisConnection,
                     boolean reconnectUsesNewConnection, Duration reconnectInterval,
                     String consumerGroup, String consumerId,
                     String prefix, XReadArgs.StreamOffset<String>[] offsets,
                     Map<String, List<Consumer<?>>> listeners,
                     boolean deleteMessages) {
    this.redisClient = redisClient;
    this.redisConnection = redisConnection;
    this.reconnectUsesNewConnection = reconnectUsesNewConnection;
    this.reconnectIntervalMillis = reconnectInterval.toMillis();
    this.consumerGroup = consumerGroup;
    this.consumerId = consumerId;
    this.prefix = prefix;
    this.offsets = offsets;
    this.listeners = listeners;
    this.deleteMessages = deleteMessages;
  }

  /** Returns a new builder to read from the Redis Streams. */
  public static Builder newBuilder(RedisClient redisClient) {
    return new ZeebeRedis.Builder(redisClient);
  }

  private void start() {
    redisConnection.addListener(new RedisConnectionStateAdapter() {
      public void onRedisConnected(RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
        LOGGER.info("Redis reconnected.");
      }
      public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
        if (externalClose) return;
        LOGGER.warn("Redis connection lost.");
        if (reconnectUsesNewConnection) {
          doClose();
          reconnectExecutorService = Executors.newSingleThreadExecutor();
          reconnectFuture = reconnectExecutorService.submit(ZeebeRedis.this::reconnect);
        }
      }
    });
    isClosed = false;
    executorService = Executors.newSingleThreadExecutor();
    future = executorService.submit(this::readFromStream);
  }

  public void reconnect() {
    ProtobufCodec protobufCodec = new ProtobufCodec();
    do {
      try {
        Thread.sleep(reconnectIntervalMillis);
        redisConnection = redisClient.connect(protobufCodec);
        LOGGER.info("Redis reconnected.");
        listeners.keySet().stream().forEach(stream -> {
          try {
            redisConnection.sync().xgroupCreate(XReadArgs.StreamOffset.from(stream, "0-0"), consumerGroup,
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

  /** Stop reading from the Redis Strams. */
  @Override
  public void close() {
    externalClose = true;
    doClose();
  }

  public void doClose() {
    LOGGER.info("Closing Consumer[group={}, id={}]. Stop reading from streams '{}*'.", consumerGroup, consumerId, prefix);

    isClosed = true;

    if (future != null) {
      future.cancel(true);
      future = null;
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
    while (!isClosed) {
      readNext();
    }
  }

  private void readNext() {
    LOGGER.trace("Consumer[id={}] reads from streams '{}*'", consumerId, prefix);

    try {
      List<StreamMessage<String, byte[]>> messages = redisConnection.sync()
              .xreadgroup(io.lettuce.core.Consumer.from(consumerGroup, consumerId),
                      XReadArgs.Builder.block(Long.MAX_VALUE), offsets);

      for (StreamMessage<String, byte[]> message : messages) {
        LOGGER.trace("Consumer[id={}] received message {} from {}", consumerId, message.getId(), message.getStream());
        var success = handleRecord(message);
        redisConnection.async().xack(message.getStream(), consumerGroup, message.getId());
        if (deleteMessages && success) {
          redisConnection.async().xdel(message.getStream(), message.getId());
        }
      }
    } catch (IllegalArgumentException ex) {
      // should not happen with a correct configuration
      LOGGER.error("Illegal arguments for xreadgroup: {}. Closing Redis client.", ex.getMessage());
      try {
        close();
      } catch (Exception closingFailure) {
        LOGGER.debug("Failure while closing the client", closingFailure);
      }
    } catch (RedisCommandTimeoutException e) {
      if (!isClosed) {
        LOGGER.debug("Consumer[group={}, id={}] timed out reading from streams '{}*'", consumerGroup, consumerId, prefix);
      }
    } catch (Exception e) {
      if (!isClosed) {
        LOGGER.error("Consumer[group={}, id={}] failed to read from streams '{}*'", consumerGroup, consumerId, prefix, e);
      }
    }
  }

  private boolean handleRecord(StreamMessage<String, byte[]> message) throws InvalidProtocolBufferException {
    final var messageValue = message.getBody().values().iterator().next();
    final var genericRecord = Schema.Record.parseFrom(messageValue);
    final var recordType = RECORD_MESSAGE_TYPES.get(message.getStream().substring(prefix.length()));

    try {
      handleRecord(message.getStream(), genericRecord, recordType);
      return true;
    } catch (InvalidProtocolBufferException e) {
      LOGGER.error("Failed to deserialize Protobuf message {} from {}", message.getId(), message.getStream(), e);
      return true; // not interested in reading corrupt data again
    } catch (Exception ex) {
      LOGGER.error("Error handling message {} from {}: {}", message.getId(), message.getStream(), ex.getMessage());
      return false;
    }
  }

  private <T extends com.google.protobuf.Message> void handleRecord(String stream,
      Schema.Record genericRecord, Class<T> t) throws InvalidProtocolBufferException {

    final var record = genericRecord.getRecord().unpack(t);

    LOGGER.trace("Consumer[id={}] handling record {}", consumerId, record);

    listeners
      .getOrDefault(stream, List.of())
      .forEach(listener -> ((Consumer<T>) listener).accept(record));

  }

  public static class Builder {

    private final RedisClient redisClient;

    private boolean reconnectUsesNewConnection = false;
    private Duration reconnectInterval = Duration.ofSeconds(1);

    private final Map<String, List<Consumer<?>>> listeners = new HashMap<>();

    private String consumerGroup = UUID.randomUUID().toString();

    private String consumerId = UUID.randomUUID().toString();
    private String prefix = "zeebe:";

    private String offset = "0-0";

    private boolean deleteMessages = false;

    private Builder(RedisClient redisClient) {
      this.redisClient = redisClient;
    }

    public Builder withReconnectUsingNewConnection() {
      this.reconnectUsesNewConnection = true;
      return this;
    }

    public Builder reconnectInterval(Duration duration) {
      this.reconnectInterval = duration;
      return this;
    }

    /** Set the consumer group, e.g. the application name. */
    public Builder consumerGroup(String consumerGroup) {
      this.consumerGroup = consumerGroup;
      return this;
    }

    /** Set the unique consumer ID. */
    public Builder consumerId(String consumerId) {
      this.consumerId = consumerId;
      return this;
    }

    /** Set the prefix for the Streams to read from. */
    public Builder prefix(String name) {
      this.prefix = name + ":";
      return this;
    }

    /** Start reading from a given offset. */
    public Builder offset(String offset) {
      this.offset = offset;
      return this;
    }

    public Builder deleteMessagesAfterSuccessfulHandling(boolean deleteMessages) {
      this.deleteMessages = deleteMessages;
      return this;
    }

    private <T extends com.google.protobuf.Message> void addListener(
        String valueType, Consumer<T> listener) {
      final var recordListeners = listeners.getOrDefault(valueType, new ArrayList<>());
      recordListeners.add(listener);
      listeners.put(prefix + valueType, recordListeners);
    }

    public Builder addDeploymentListener(Consumer<Schema.DeploymentRecord> listener) {
      addListener(ValueType.DEPLOYMENT.name(), listener);
      return this;
    }

    public Builder addDeploymentDistributionListener(
        Consumer<Schema.DeploymentDistributionRecord> listener) {
      addListener(ValueType.DEPLOYMENT_DISTRIBUTION.name(), listener);
      return this;
    }

    public Builder addProcessListener(Consumer<Schema.ProcessRecord> listener) {
      addListener(ValueType.PROCESS.name(), listener);
      return this;
    }

    public Builder addProcessInstanceListener(Consumer<Schema.ProcessInstanceRecord> listener) {
      addListener(ValueType.PROCESS_INSTANCE.name(), listener);
      return this;
    }

    public Builder addProcessEventListener(Consumer<Schema.ProcessEventRecord> listener) {
      addListener(ValueType.PROCESS_EVENT.name(), listener);
      return this;
    }

    public Builder addVariableListener(Consumer<Schema.VariableRecord> listener) {
      addListener(ValueType.VARIABLE.name(), listener);
      return this;
    }

    public Builder addVariableDocumentListener(Consumer<Schema.VariableDocumentRecord> listener) {
      addListener(ValueType.VARIABLE_DOCUMENT.name(), listener);
      return this;
    }

    public Builder addJobListener(Consumer<Schema.JobRecord> listener) {
      addListener(ValueType.JOB.name(), listener);
      return this;
    }

    public Builder addJobBatchListener(Consumer<Schema.JobBatchRecord> listener) {
      addListener(ValueType.JOB_BATCH.name(), listener);
      return this;
    }

    public Builder addIncidentListener(Consumer<Schema.IncidentRecord> listener) {
      addListener(ValueType.INCIDENT.name(), listener);
      return this;
    }

    public Builder addTimerListener(Consumer<Schema.TimerRecord> listener) {
      addListener(ValueType.TIMER.name(), listener);
      return this;
    }

    public Builder addMessageListener(Consumer<Schema.MessageRecord> listener) {
      addListener(ValueType.MESSAGE.name(), listener);
      return this;
    }

    public Builder addMessageSubscriptionListener(
        Consumer<Schema.MessageSubscriptionRecord> listener) {
      addListener(ValueType.MESSAGE_SUBSCRIPTION.name(), listener);
      return this;
    }

    public Builder addMessageStartEventSubscriptionListener(
        Consumer<Schema.MessageStartEventSubscriptionRecord> listener) {
      addListener(ValueType.MESSAGE_START_EVENT_SUBSCRIPTION.name(), listener);
      return this;
    }

    public Builder addProcessMessageSubscriptionListener(
        Consumer<Schema.ProcessMessageSubscriptionRecord> listener) {
      addListener(ValueType.PROCESS_MESSAGE_SUBSCRIPTION.name(), listener);
      return this;
    }

    public Builder addProcessInstanceCreationListener(
        Consumer<Schema.ProcessInstanceCreationRecord> listener) {
      addListener(ValueType.PROCESS_INSTANCE_CREATION.name(), listener);
      return this;
    }

    public Builder addErrorListener(Consumer<Schema.ErrorRecord> listener) {
      addListener(ValueType.ERROR.name(), listener);
      return this;
    }

    /**
     * Start a background task that reads from Zeebe Streams.
     * <br>
     * Call {@link #close()} to stop reading.
     */
    public ZeebeRedis build() {
      if (listeners.size() == 0) {
        throw new IllegalArgumentException("Must register a least one listener, but none found.");
      }


      final var connection = redisClient.connect(new ProtobufCodec());

      LOGGER.info("Read from streams '{}*' with offset '{}'", prefix, offset);

      // Prepare
      List<XReadArgs.StreamOffset<String>> offsets = new ArrayList<>();
      listeners.keySet().stream().forEach(stream -> {
        offsets.add(XReadArgs.StreamOffset.lastConsumed(stream));
        try {
          connection.sync().xgroupCreate(XReadArgs.StreamOffset.from(stream, offset), consumerGroup,
                  XGroupCreateArgs.Builder.mkstream());
        } catch (RedisBusyException ex) {
          // NOOP: consumer group already exists
        }
      });

      final var zeebeRedis = new ZeebeRedis(redisClient, connection, reconnectUsesNewConnection, reconnectInterval,
              consumerGroup, consumerId, prefix, offsets.toArray(new XReadArgs.StreamOffset[0]), listeners,
              deleteMessages);
      zeebeRedis.start();

      return zeebeRedis;
    }
  }
}
