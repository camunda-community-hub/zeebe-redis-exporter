package io.zeebe.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.redis.connect.java.ZeebeRedis;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import java.util.ArrayList;
import java.util.List;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ExporterTest {

  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess("process")
          .startEvent("start")
          .sequenceFlowId("to-task")
          .serviceTask("task", s -> s.zeebeJobType("test").zeebeInputExpression("foo", "bar"))
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

  private ZeebeClient client;
  private ZeebeRedis zeebeRedis1;

  private ZeebeRedis zeebeRedis2;

  @Container public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withDefaultConfig();

  private RedisClient redisClient;

  @BeforeEach
  public void init() {
    client = zeebeContainer.getClient();
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
  }

  @AfterEach
  public void cleanUp() {
    if (zeebeRedis1 != null) zeebeRedis1.close();
    if (zeebeRedis2 != null) zeebeRedis2.close();
    redisClient.shutdown();
  }

  @Test
  public void shouldListenToDeploymentAndProcessEvents() {
    // given
    final List<Schema.DeploymentRecord> deploymentRecords = new ArrayList<>();
    final List<Schema.ProcessInstanceRecord> processInstanceRecords = new ArrayList<>();

    zeebeRedis1 =
        ZeebeRedis.newBuilder(redisClient)
            .consumerGroup("ExporterTest")
            .consumerId("consumer-1")
            .addDeploymentListener(deploymentRecords::add)
            .addProcessInstanceListener(processInstanceRecords::add)
            .build();

    // when
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process.bpmn").send().join();

    // then
    Awaitility.await("await until the deployment is created")
        .untilAsserted(
            () ->
                assertThat(deploymentRecords)
                    .extracting(r -> r.getMetadata().getIntent())
                    .contains(DeploymentIntent.CREATED.name()));

    // when
    client.newCreateInstanceCommand().bpmnProcessId("process").latestVersion().send().join();

    // then
    Awaitility.await("await until the service task is activated")
        .untilAsserted(
            () ->
                assertThat(processInstanceRecords)
                    .extracting(Schema.ProcessInstanceRecord::getBpmnElementType)
                    .contains(BpmnElementType.SERVICE_TASK.name()));
  }

  @Test
  public void shouldDeliverDifferentMessagesPerConsumerId() {
    // given
    final List<Schema.DeploymentRecord> records1 = new ArrayList<>();
    final List<Schema.DeploymentRecord> records2 = new ArrayList<>();

    zeebeRedis1 =
        ZeebeRedis.newBuilder(redisClient)
            .consumerGroup("ExporterTest")
            .consumerId("consumer-1")
            .addDeploymentListener(records1::add)
            .build();
    zeebeRedis2 =
        ZeebeRedis.newBuilder(redisClient)
            .consumerGroup("ExporterTest")
            .consumerId("consumer-2")
            .addDeploymentListener(records2::add)
            .build();

    // when
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process1.bpmn").send().join();
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process2.bpmn").send().join();
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process3.bpmn").send().join();
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process4.bpmn").send().join();

    // then
    Awaitility.await("await until all deployments are created")
        .untilAsserted(
            () -> {
              var allRecords = new ArrayList<>(records1);
              allRecords.addAll(records2);
              assertThat(allRecords)
                  .extracting(r -> r.getMetadata().getIntent())
                  .filteredOn(i -> i.equals(DeploymentIntent.CREATED.name()))
                  .hasSize(4);
            });

    assertThat(records1.size()).isGreaterThan(0);
    assertThat(records2.size()).isGreaterThan(0);
    assertThat(records1).doesNotContainAnyElementsOf(records2);
  }

  @Test
  public void shouldDeleteConsumerIdIfNotSetExplicitly() {
    // given
    final List<Schema.DeploymentRecord> records1 = new ArrayList<>();
    final List<Schema.DeploymentRecord> records2 = new ArrayList<>();

    zeebeRedis1 =
        ZeebeRedis.newBuilder(redisClient)
            .consumerGroup("ExporterTest")
            .addDeploymentListener(records1::add)
            .build();
    zeebeRedis2 =
        ZeebeRedis.newBuilder(redisClient)
            .consumerGroup("ExporterTest")
            .consumerId("consumer-2")
            .addDeploymentListener(records2::add)
            .build();

    // create some events
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process1.bpmn").send().join();
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process2.bpmn").send().join();
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process3.bpmn").send().join();
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process4.bpmn").send().join();

    // and consume them
    Awaitility.await("await until all deployments are created")
        .untilAsserted(
            () -> {
              var allRecords = new ArrayList<>(records1);
              allRecords.addAll(records2);
              assertThat(allRecords)
                  .extracting(r -> r.getMetadata().getIntent())
                  .filteredOn(i -> i.equals(DeploymentIntent.CREATED.name()))
                  .hasSize(4);
            });

    // assert that 2 consumers are registered
    RedisStreamCommands<String, String> streamCommands = redisClient.connect().sync();
    var consumers = streamCommands.xinfoConsumers("zeebe:DEPLOYMENT", "ExporterTest");
    assertThat(consumers).hasSize(2);

    // when
    zeebeRedis1.close();
    zeebeRedis2.close();

    // then
    // assert that only the second consumer is left and the first (where the ID is auto generated)
    // has been deleted
    consumers = streamCommands.xinfoConsumers("zeebe:DEPLOYMENT", "ExporterTest");
    assertThat(consumers).hasSize(1);
    assertThat(consumers).allMatch(c -> c.toString().contains("consumer-2"));
  }

  @Test
  public void shouldListenToUserTaskEvents() {
    // given
    final List<Schema.DeploymentRecord> deploymentRecords = new ArrayList<>();
    final List<Schema.UserTaskRecord> userTaskRecords = new ArrayList<>();

    zeebeRedis1 =
        ZeebeRedis.newBuilder(redisClient)
            .consumerGroup("ExporterTest")
            .consumerId("consumer-1")
            .addDeploymentListener(deploymentRecords::add)
            .addUserTaskListener(userTaskRecords::add)
            .build();

    // when
    client
        .newDeployResourceCommand()
        .addProcessModel(USER_TASK_WORKFLOW, "user-task.bpmn")
        .send()
        .join();

    // then
    Awaitility.await("await until the deployment is created")
        .untilAsserted(
            () ->
                assertThat(deploymentRecords)
                    .extracting(r -> r.getMetadata().getIntent())
                    .contains(DeploymentIntent.CREATED.name()));

    // when
    client
        .newCreateInstanceCommand()
        .bpmnProcessId("user_task_process")
        .latestVersion()
        .send()
        .join();

    // then
    Awaitility.await("await until the user task is activated")
        .untilAsserted(
            () ->
                assertThat(userTaskRecords)
                    .extracting(Schema.UserTaskRecord::getElementId)
                    .contains("userTask"));

    assertThat(userTaskRecords.size()).isGreaterThan(0);
    assertThat(userTaskRecords)
        .filteredOn(u -> u.getElementId().equals("userTask"))
        .first()
        .satisfies(
            u -> {
              assertThat(u.getCandidateGroupsCount()).isEqualTo(1);
              assertThat(u.getCandidateGroups(0)).isEqualTo("testGroup");
            });
  }
}
