package io.zeebe.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.redis.exporter.ProtobufCodec;
import io.zeebe.redis.testcontainers.OnFailureExtension;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(OnFailureExtension.class)
public class ExporterMinTimeToLiveTest {

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
          .doDeleteAfterAcknowledge(true)
          .andUseMinTTLInSeconds(10);

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
  public void shouldConsiderMinTtlWhenDeleteAfterAcknowledge() throws Exception {
    // given: some consumed and acknowledged messages
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process-1.bpmn")
        .send()
        .join();
    Thread.sleep(1000);
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process-2.bpmn")
        .send()
        .join();
    Thread.sleep(1000);
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process-3.bpmn")
        .send()
        .join();
    redisConnection
        .sync()
        .xgroupCreate(
            XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_1",
            XGroupCreateArgs.Builder.mkstream());
    Thread.sleep(1000);
    var messages =
        redisConnection
            .sync()
            .xreadgroup(
                Consumer.from("application_1", "consumer_1"),
                XReadArgs.Builder.block(6000),
                XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(messages.size()).isGreaterThan(0);
    var xlen = redisConnection.sync().xlen("zeebe:DEPLOYMENT");
    for (StreamMessage<String, byte[]> message : messages) {
      redisConnection.async().xack("zeebe:DEPLOYMENT", "application_1", message.getId());
    }
    ;

    // when: cleanupHasRun but min TTL has not been reached
    Thread.sleep(6000);

    // then: cleanup did not yet remove the messages
    assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isEqualTo(xlen);

    // but will delete them after min TTL
    var delay = Duration.ofSeconds(5);
    await()
        .atMost(Duration.ofSeconds(12))
        .pollDelay(delay)
        .pollInterval(Duration.ofMillis(1000))
        .pollInSameThread()
        .untilAsserted(
            () -> assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isLessThan(xlen));
  }
}
