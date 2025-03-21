package io.zeebe.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import io.lettuce.core.RedisClient;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.redis.connect.java.ProtobufCodec;
import io.zeebe.redis.connect.java.ZeebeRedis;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class RedisUnavailabilityTest {

  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess("process")
          .startEvent("start")
          .sequenceFlowId("to-task")
          .serviceTask("task", s -> s.zeebeJobType("test").zeebeInputExpression("foo", "bar"))
          .sequenceFlowId("to-end")
          .endEvent("end")
          .done();

  private ZeebeRedis zeebeRedis;

  private ZeebeClient client;

  @Container public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withDefaultConfig();

  private RedisClient redisClient;

  @BeforeEach
  public void init() {
    client = zeebeContainer.getClient();

    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
  }

  @AfterEach
  public void cleanUp() {
    if (zeebeRedis != null) zeebeRedis.close();
    redisClient.shutdown();
  }

  @Test
  public void shouldReconnectIfRedisIsTempUnavailable() throws Exception {
    final List<Schema.DeploymentRecord> deploymentRecords = new ArrayList<>();

    // given
    zeebeRedis =
        ZeebeRedis.newBuilder(redisClient)
            .withReconnectUsingNewConnection()
            .reconnectInterval(Duration.ofSeconds(1))
            .deleteMessagesAfterSuccessfulHandling(true)
            .addDeploymentListener(deploymentRecords::add)
            .build();

    // when
    zeebeContainer.getRedisContainer().stop();
    Thread.sleep(2000);
    client.newDeployResourceCommand().addProcessModel(PROCESS, "process.bpmn").send().join();
    Thread.sleep(1000);
    zeebeContainer.getRedisContainer().start();
    Thread.sleep(2000);

    // then
    Awaitility.await("await until the deployment is created")
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(
            () ->
                assertThat(deploymentRecords)
                    .extracting(r -> r.getMetadata().getIntent())
                    .contains(DeploymentIntent.CREATED.name()));

    var redisConnection = redisClient.connect(new ProtobufCodec());
    Awaitility.await("await until all messages of deployment stream have been deleted")
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isEqualTo(0));
    redisConnection.close();
  }
}
