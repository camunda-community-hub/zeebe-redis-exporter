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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(OnFailureExtension.class)
public class ExporterTest {

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process")
          .startEvent("start")
          .sequenceFlowId("to-task")
          .serviceTask("task", s -> s.zeebeJobType("test"))
          .sequenceFlowId("to-end")
          .endEvent("end")
          .done();

  private static final BpmnModelInstance USER_TASK_WORKFLOW =
      Bpmn.createExecutableProcess("user_task_process")
          .startEvent("start")
          .sequenceFlowId("to-task")
          .userTask("userTask", u -> u.zeebeUserTask().zeebeCandidateGroups("testGroup"))
          .sequenceFlowId("to-end")
          .endEvent("end")
          .done();

  @Container public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withDefaultConfig();

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
  public void shouldExportEventsAsProtobuf() throws Exception {
    // given
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process.bpmn")
        .send()
        .join();
    Thread.sleep(1000);

    // when
    final var message =
        redisConnection.sync().xrange("zeebe:DEPLOYMENT", Range.create("-", "+")).get(0);

    // then
    assertThat(message).isNotNull();
    final var messageValue = message.getBody().values().iterator().next();

    final var record = Schema.Record.parseFrom(messageValue);
    assertThat(record.getRecord().is(Schema.DeploymentRecord.class)).isTrue();

    final var deploymentRecord = record.getRecord().unpack(Schema.DeploymentRecord.class);
    final Schema.DeploymentRecord.DeploymentResource resource = deploymentRecord.getResources(0);
    assertThat(resource.getResourceName()).isEqualTo("process.bpmn");
  }

  @Test
  public void shouldSupportConsumerGroups() throws Exception {
    // given
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process.bpmn")
        .send()
        .join();
    redisConnection
        .sync()
        .xgroupCreate(
            XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_1",
            XGroupCreateArgs.Builder.mkstream());
    Thread.sleep(1000);

    // when
    var messages =
        redisConnection
            .sync()
            .xreadgroup(
                Consumer.from("application_1", "consumer_1"),
                XReadArgs.Builder.block(6000),
                XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));

    // then
    assertThat(messages.size()).isGreaterThan(0);
    for (StreamMessage<String, byte[]> message : messages) {
      final var messageValue = message.getBody().values().iterator().next();

      final var record = Schema.Record.parseFrom(messageValue);
      assertThat(record.getRecord().is(Schema.DeploymentRecord.class)).isTrue();

      redisConnection.async().xack("zeebe:DEPLOYMENT", "application_1", message.getId());
    }
    ;

    messages =
        redisConnection
            .sync()
            .xreadgroup(
                Consumer.from("application_1", "consumer_1"),
                XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(messages.size()).isEqualTo(0);
  }

  @Test
  public void shouldExportUserTaskEvents() throws Exception {
    // given
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(USER_TASK_WORKFLOW, "user-task.bpmn")
        .send()
        .join();
    zeebeContainer
        .getClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("user_task_process")
        .latestVersion()
        .send()
        .join();
    Thread.sleep(1000);

    // when
    final var message =
        redisConnection.sync().xrange("zeebe:USER_TASK", Range.create("-", "+")).get(0);

    // then
    assertThat(message).isNotNull();
    final var messageValue = message.getBody().values().iterator().next();

    final var record = Schema.Record.parseFrom(messageValue);
    assertThat(record.getRecord().is(Schema.UserTaskRecord.class)).isTrue();

    final var userTaskRecord = record.getRecord().unpack(Schema.UserTaskRecord.class);
    assertThat(userTaskRecord.getElementId()).isEqualTo("userTask");
    assertThat(userTaskRecord.getCandidateGroupsCount()).isEqualTo(1);
    assertThat(userTaskRecord.getCandidateGroups(0)).isEqualTo("testGroup");
  }
}
