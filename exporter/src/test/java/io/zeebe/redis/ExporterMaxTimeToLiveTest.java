package io.zeebe.redis;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.redis.exporter.ProtobufCodec;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ExporterMaxTimeToLiveTest {

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process")
          .startEvent("start")
          .sequenceFlowId("to-task")
          .serviceTask("task", s -> s.zeebeJobType("test"))
          .sequenceFlowId("to-end")
          .endEvent("end")
          .done();

  @Container
  public ZeebeTestContainer zeebeContainer = ZeebeTestContainer
          .withCleanupCycleInSeconds(2).withMaxTTLInSeconds(6);

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
  public void shouldConsiderMaxTimeToLive() throws Exception {
    // given
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process.bpmn").send().join();
    Thread.sleep(1000);
    final var message = redisConnection.sync()
            .xrange("zeebe:DEPLOYMENT", Range.create("-", "+")).get(0);
    assertThat(message).isNotNull();
    var deploymentLen = redisConnection.sync().xlen("zeebe:DEPLOYMENT");
    assertThat(deploymentLen).isGreaterThan(0);

    // when
    Thread.sleep(3000);

    // then
    assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isEqualTo(deploymentLen);
    Thread.sleep(5000);
    Awaitility.await().pollInSameThread().pollInterval(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
            .untilAsserted(() -> assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isEqualTo(0));
  }
}
