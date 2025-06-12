package io.zeebe.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.redis.exporter.ProtobufCodec;
import io.zeebe.redis.testcontainers.OnFailureExtension;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import java.util.ArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(OnFailureExtension.class)
public class TransferAnbandonedMessagesTest {

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process")
          .startEvent("start")
          .sequenceFlowId("to-task")
          .serviceTask("task", s -> s.zeebeJobType("test"))
          .sequenceFlowId("to-end")
          .endEvent("end")
          .done();

  @Container
  public ZeebeTestContainer zeebeContainer =
      ZeebeTestContainer.withCleanupCycleInSeconds(3)
          .andUseConsumerJobTimeoutInSeconds(5)
          .doDeleteAfterAcknowledge(true);

  private RedisClient redisClient;
  private StatefulRedisConnection<String, byte[]> redisConnection;

  @BeforeEach
  public void init() {
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
    redisConnection = redisClient.connect(new ProtobufCodec());
    redisConnection.sync().xtrim("zeebe:DEPLOYMENT", 0);
  }

  @AfterEach
  public void cleanUp() {
    redisConnection.sync().xtrim("zeebe:DEPLOYMENT", 0);
    redisConnection.close();
    redisClient.shutdown();
  }

  @Test
  public void shouldTransferPendingMessagesAfterJobTimeout() throws Exception {
    // given: messages consumed and acknowledged by only one of two consumer groups
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process-1.bpmn")
        .send()
        .join();
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process-2.bpmn")
        .send()
        .join();
    redisConnection
        .sync()
        .xgroupCreate(
            XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_42",
            XGroupCreateArgs.Builder.mkstream());
    Thread.sleep(1000);
    // read some messages from consumer 1
    var pendingMessages =
        redisConnection
            .sync()
            .xreadgroup(
                Consumer.from("application_42", "consumer_1"),
                XReadArgs.Builder.block(5000),
                XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(pendingMessages.size()).isGreaterThan(0);
    // do not xack messages so that they are pending

    // create more messages
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process-3.bpmn")
        .send()
        .join();
    Thread.sleep(1000);
    // read new messages from consumer 2 so that this is the youngest consumer
    var messages =
        redisConnection
            .sync()
            .xreadgroup(
                Consumer.from("application_42", "consumer_2"),
                XReadArgs.Builder.block(3000),
                XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(messages.size()).isGreaterThan(0);
    for (StreamMessage<String, byte[]> message : messages) {
      redisConnection.async().xack("zeebe:DEPLOYMENT", "application_42", message.getId());
    }

    // when: cleanupHasRun and pending messages have reached the job timeout
    Thread.sleep(6000);
    messages =
        redisConnection
            .sync()
            .xreadgroup(
                Consumer.from("application_42", "consumer_2"),
                XReadArgs.Builder.block(2000),
                XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(messages.size()).isGreaterThan(0);
    var messageKeys = new ArrayList<String>();
    for (StreamMessage<String, byte[]> message : messages) {
      redisConnection.async().xack("zeebe:DEPLOYMENT", "application_42", message.getId());
      final var messageValue = message.getBody().values().iterator().next();
      final var record = Schema.Record.parseFrom(messageValue);
      assertThat(record.getRecord().is(Schema.DeploymentRecord.class)).isTrue();
      messageKeys.addAll(message.getBody().keySet());
    }
    // assert that former pending messages have been consumed by consumer 2
    for (StreamMessage<String, byte[]> pending : pendingMessages) {
      assertThat(messageKeys).containsAll(pending.getBody().keySet());
    }
  }
}
