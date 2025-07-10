package io.zeebe.redis.connect.java;

import io.camunda.zeebe.protocol.record.ValueType;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.zeebe.exporter.proto.Schema;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisConnectionBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnectionBuilder.class);

  private static final int XREAD_BLOCK_MILLISECONDS = 2000;
  private static final int XREAD_COUNT = 500;
  private final UniversalRedisClient redisClient;

  private boolean reconnectUsesNewConnection = false;
  private Duration reconnectInterval = Duration.ofSeconds(1);

  private final Map<String, List<Consumer<?>>> listeners = new HashMap<>();

  private String consumerGroup = UUID.randomUUID().toString();

  private boolean shouldDestroyConsumerGroupOnClose = true;

  private String consumerId = UUID.randomUUID().toString();
  private boolean shouldDeleteConsumerIdOnClose = true;

  private String prefix = "zeebe:";

  private String offset = "0-0";

  private int xreadBlockMillis = XREAD_BLOCK_MILLISECONDS;

  private int xreadCount = XREAD_COUNT;

  private boolean deleteMessages = false;

  RedisConnectionBuilder(RedisClient redisClient) {
    this.redisClient = new UniversalRedisClient(redisClient);
  }

  RedisConnectionBuilder(RedisClusterClient redisClient) {
    this.redisClient = new UniversalRedisClient(redisClient);
  }

  public RedisConnectionBuilder withReconnectUsingNewConnection() {
    this.reconnectUsesNewConnection = true;
    return this;
  }

  public RedisConnectionBuilder withStandardClusterOptions() {
    this.redisClient.setStandardClusterOptions();
    return this;
  }

  public RedisConnectionBuilder reconnectInterval(Duration duration) {
    this.reconnectInterval = duration;
    return this;
  }

  /** Sets the XREAD [BLOCK milliseconds] parameter. Default is 2000. */
  public RedisConnectionBuilder xreadBlockMillis(int xreadBlockMillis) {
    this.xreadBlockMillis = xreadBlockMillis;
    return this;
  }

  /** Sets the XREAD [COUNT count] parameter. Default is 1000. */
  public RedisConnectionBuilder xreadCount(int xreadCount) {
    this.xreadCount = xreadCount;
    return this;
  }

  /** Set the consumer group, e.g. the application name. */
  public RedisConnectionBuilder consumerGroup(String consumerGroup) {
    this.consumerGroup = consumerGroup;
    this.shouldDestroyConsumerGroupOnClose = false;
    return this;
  }

  /** Set the unique consumer ID. */
  public RedisConnectionBuilder consumerId(String consumerId) {
    this.consumerId = consumerId;
    this.shouldDeleteConsumerIdOnClose = false;
    return this;
  }

  /** Set the prefix for the Streams to read from. */
  public RedisConnectionBuilder prefix(String name) {
    this.prefix = name + ":";
    return this;
  }

  /** Start reading from a given offset. */
  public RedisConnectionBuilder offset(String offset) {
    this.offset = offset;
    return this;
  }

  public RedisConnectionBuilder deleteMessagesAfterSuccessfulHandling(boolean deleteMessages) {
    this.deleteMessages = deleteMessages;
    return this;
  }

  private <T extends com.google.protobuf.Message> void addListener(
      String valueType, Consumer<T> listener) {
    final var recordListeners = listeners.getOrDefault(valueType, new ArrayList<>());
    recordListeners.add(listener);
    listeners.put(prefix + valueType, recordListeners);
  }

  public RedisConnectionBuilder addDeploymentListener(Consumer<Schema.DeploymentRecord> listener) {
    addListener(ValueType.DEPLOYMENT.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addDeploymentDistributionListener(
      Consumer<Schema.DeploymentDistributionRecord> listener) {
    addListener(ValueType.DEPLOYMENT_DISTRIBUTION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addProcessListener(Consumer<Schema.ProcessRecord> listener) {
    addListener(ValueType.PROCESS.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addProcessInstanceListener(
      Consumer<Schema.ProcessInstanceRecord> listener) {
    addListener(ValueType.PROCESS_INSTANCE.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addProcessEventListener(
      Consumer<Schema.ProcessEventRecord> listener) {
    addListener(ValueType.PROCESS_EVENT.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addVariableListener(Consumer<Schema.VariableRecord> listener) {
    addListener(ValueType.VARIABLE.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addVariableDocumentListener(
      Consumer<Schema.VariableDocumentRecord> listener) {
    addListener(ValueType.VARIABLE_DOCUMENT.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addJobListener(Consumer<Schema.JobRecord> listener) {
    addListener(ValueType.JOB.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addJobBatchListener(Consumer<Schema.JobBatchRecord> listener) {
    addListener(ValueType.JOB_BATCH.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addIncidentListener(Consumer<Schema.IncidentRecord> listener) {
    addListener(ValueType.INCIDENT.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addTimerListener(Consumer<Schema.TimerRecord> listener) {
    addListener(ValueType.TIMER.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addMessageListener(Consumer<Schema.MessageRecord> listener) {
    addListener(ValueType.MESSAGE.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addMessageSubscriptionListener(
      Consumer<Schema.MessageSubscriptionRecord> listener) {
    addListener(ValueType.MESSAGE_SUBSCRIPTION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addMessageStartEventSubscriptionListener(
      Consumer<Schema.MessageStartEventSubscriptionRecord> listener) {
    addListener(ValueType.MESSAGE_START_EVENT_SUBSCRIPTION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addProcessMessageSubscriptionListener(
      Consumer<Schema.ProcessMessageSubscriptionRecord> listener) {
    addListener(ValueType.PROCESS_MESSAGE_SUBSCRIPTION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addProcessInstanceCreationListener(
      Consumer<Schema.ProcessInstanceCreationRecord> listener) {
    addListener(ValueType.PROCESS_INSTANCE_CREATION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addErrorListener(Consumer<Schema.ErrorRecord> listener) {
    addListener(ValueType.ERROR.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addFormListener(Consumer<Schema.FormRecord> listener) {
    addListener(ValueType.FORM.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addResourceDeletionListener(
      Consumer<Schema.ResourceDeletionRecord> listener) {
    addListener(ValueType.RESOURCE_DELETION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addUserTaskListener(Consumer<Schema.UserTaskRecord> listener) {
    addListener(ValueType.USER_TASK.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addCompensationSubscriptionListener(
      Consumer<Schema.CompensationSubscriptionRecord> listener) {
    addListener(ValueType.COMPENSATION_SUBSCRIPTION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addEscalationListener(Consumer<Schema.EscalationRecord> listener) {
    addListener(ValueType.ESCALATION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addProcessInstanceMigrationListener(Consumer<Schema.ProcessInstanceMigrationRecord> listener) {
    addListener(ValueType.PROCESS_INSTANCE_MIGRATION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addProcessInstanceBatchListener(Consumer<Schema.ProcessInstanceBatchRecord> listener) {
    addListener(ValueType.PROCESS_INSTANCE_BATCH.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addProcessInstanceResultListener(Consumer<Schema.ProcessInstanceResultRecord> listener) {
    addListener(ValueType.PROCESS_INSTANCE_RESULT.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addResourceListener(Consumer<Schema.ResourceRecord> listener) {
    addListener(ValueType.RESOURCE.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addMessageBatchListener(Consumer<Schema.MessageBatchRecord> listener) {
    addListener(ValueType.MESSAGE_BATCH.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addMessageCorrelationListener(Consumer<Schema.MessageCorrelationRecord> listener) {
    addListener(ValueType.MESSAGE_CORRELATION.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addClockListener(Consumer<Schema.ClockRecord> listener) {
    addListener(ValueType.CLOCK.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addUserListener(Consumer<Schema.UserRecord> listener) {
    addListener(ValueType.USER.name(), listener);
    return this;
  }

  public RedisConnectionBuilder addAuthorizationListener(Consumer<Schema.AuthorizationRecord> listener) {
    addListener(ValueType.AUTHORIZATION.name(), listener);
    return this;
  }

  /**
   * Start a background task that reads from Zeebe Streams. <br>
   * Call {@link #close()} to stop reading.
   */
  public ZeebeRedis build() {
    if (listeners.size() == 0) {
      throw new IllegalArgumentException("Must register a least one listener, but none found.");
    }

    final var connection = redisClient.connect(new ProtobufCodec());

    LOGGER.info("Read from Redis streams '{}*' with offset '{}'", prefix, offset);

    // Prepare
    var syncStreamCommands = connection.syncStreamCommands();
    List<XReadArgs.StreamOffset<String>> offsets = new ArrayList<>();
    listeners.keySet().stream()
        .forEach(
            stream -> {
              offsets.add(XReadArgs.StreamOffset.lastConsumed(stream));
              try {
                syncStreamCommands.xgroupCreate(
                    XReadArgs.StreamOffset.from(stream, offset),
                    consumerGroup,
                    XGroupCreateArgs.Builder.mkstream());
              } catch (RedisBusyException ex) {
                // NOOP: consumer group already exists
              }
            });

    final var zeebeRedis =
        new ZeebeRedis(
            redisClient,
            connection,
            reconnectUsesNewConnection,
            reconnectInterval,
            xreadBlockMillis,
            xreadCount,
            consumerGroup,
            consumerId,
            prefix,
            offsets.toArray(new XReadArgs.StreamOffset[0]),
            listeners,
            deleteMessages,
            shouldDestroyConsumerGroupOnClose,
            shouldDeleteConsumerIdOnClose);
    zeebeRedis.start();

    return zeebeRedis;
  }
}
