package io.zeebe.redis;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.lettuce.core.RedisClient;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.redis.connect.java.ZeebeRedis;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

  private ZeebeClient client;
  private ZeebeRedis zeebeRedis1;

  private ZeebeRedis zeebeRedis2;

  @Container
  public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withDefaultConfig();

  private RedisClient redisClient;

  @BeforeEach
  public void init() {
    client = zeebeContainer.getClient();
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
  }

  @AfterEach
  public void cleanUp() throws Exception {
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
            .consumerGroup("ExporterTest").consumerId("consumer-1")
            .addDeploymentListener(deploymentRecords::add)
            .addProcessInstanceListener(processInstanceRecords::add)
            .build();

    // when
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process.bpmn").send().join();

    // then
    Awaitility.await("await until the deployment is fully distributed")
        .untilAsserted(
            () ->
                assertThat(deploymentRecords)
                    .extracting(r -> r.getMetadata().getIntent())
                    .contains(DeploymentIntent.FULLY_DISTRIBUTED.name()));

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
                    .consumerGroup("ExporterTest").consumerId("consumer-1")
                    .addDeploymentListener(records1::add)
                    .build();
    zeebeRedis2 =
            ZeebeRedis.newBuilder(redisClient)
                    .consumerGroup("ExporterTest").consumerId("consumer-2")
                    .addDeploymentListener(records2::add)
                    .build();

    // when
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process1.bpmn").send().join();
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process2.bpmn").send().join();
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process3.bpmn").send().join();
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process4.bpmn").send().join();

    // then
    Awaitility.await("await until all deployments are fully distributed")
            .untilAsserted(() -> {
              var allRecords = new ArrayList<>(records1);
              allRecords.addAll(records2);
              assertThat(allRecords)
                      .extracting(r -> r.getMetadata().getIntent())
                      .filteredOn(i -> i.equals(DeploymentIntent.FULLY_DISTRIBUTED.name()))
                      .hasSize(4);
            });

    assertThat(records1.size()).isGreaterThan(0);
    assertThat(records2.size()).isGreaterThan(0);
    assertThat(records1).doesNotContainAnyElementsOf(records2);
  }
}
