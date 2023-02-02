package io.zeebe.redis;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.redis.connect.java.ProtobufCodec;
import io.zeebe.redis.connect.java.ZeebeRedis;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ZeebeRedisClientTest {

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

  @Container
  public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withDefaultConfig();

  private RedisClient redisClient;

  @BeforeEach
  public void init() {
    client = zeebeContainer.getClient();

    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
    redisClient.setOptions(ClientOptions.builder()
                    .autoReconnect(true)
                    .pingBeforeActivateConnection(true)
                    .timeoutOptions(TimeoutOptions.builder()
                            .fixedTimeout(Duration.ofMinutes(5))
                            .build())
            .build());
  }

  @AfterEach
  public void cleanUp() {
    if (zeebeRedis != null) zeebeRedis.close();
    redisClient.shutdown();
  }

  @Test
  public void shouldReconnectIfRedisIsTempUnavailable() throws Exception{
    final List<Schema.DeploymentRecord> deploymentRecords = new ArrayList<>();

   // given
    zeebeRedis = ZeebeRedis.newBuilder(redisClient)
            .withReconnectUsingNewConnection().reconnectInterval(Duration.ofSeconds(1))
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
    Awaitility.await("await until the deployment is fully distributed")
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() ->  assertThat(deploymentRecords)
                    .extracting(r -> r.getMetadata().getIntent())
                    .contains(DeploymentIntent.FULLY_DISTRIBUTED.name()));

    var redisConnection = redisClient.connect(new ProtobufCodec());
    Awaitility.await("await until all messages of deployment stream have been deleted")
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() ->
                assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isEqualTo(0));
    redisConnection.close();
  }
}
