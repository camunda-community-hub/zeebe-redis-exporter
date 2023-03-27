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

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ExporterTimeToLiveTest {

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
          .withCleanupCycleInSeconds(4).withMaxTTLInSeconds(4);

  private RedisClient redisClient;
  private StatefulRedisConnection<String, byte[]> redisConnection;

  @BeforeEach
  public void init() {
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
    redisConnection = redisClient.connect(new ProtobufCodec());
  }

  @AfterEach
  public void cleanUp() {
    redisConnection.sync().xtrim("zeebe:DEPLOYMENT", 0);
    redisConnection.close();
    redisClient.shutdown();
  }

  @Test
  public void shouldConsiderTimeToLive() throws Exception {
    // given
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process.bpmn").send().join();
    final var message = redisConnection.sync()
            .xrange("zeebe:DEPLOYMENT", Range.create("-", "+")).get(0);
    assertThat(message).isNotNull();
    Thread.sleep(1000);
    var deploymentLen = redisConnection.sync().xlen("zeebe:DEPLOYMENT");
    assertThat(deploymentLen).isGreaterThan(0);

    // when
    Thread.sleep(1000);
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process2.bpmn").send().join();
    Thread.sleep(1000);
    var deploymentLen2 = redisConnection.sync().xlen("zeebe:DEPLOYMENT");
    Thread.sleep(3000);

    // then
    assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isEqualTo(deploymentLen2 - deploymentLen);
  }
}
