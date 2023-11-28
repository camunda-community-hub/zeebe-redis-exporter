package io.zeebe.redis;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.redis.exporter.ProtobufCodec;
import io.zeebe.redis.testcontainers.OnFailureExtension;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@Testcontainers
@ExtendWith(OnFailureExtension.class)
public class ExporterDeleteAfterAcknowledgeTest {

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
          .withCleanupCycleInSeconds(2).doDeleteAfterAcknowledge(true);

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
  public void shouldDeleteAfterAcknowledge() throws Exception {
    // given: some consumed and acknowledged messages
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process-1.bpmn").send().join();
    Thread.sleep(1000);
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process-2.bpmn").send().join();
    redisConnection.sync().xgroupCreate(XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_1", XGroupCreateArgs.Builder.mkstream());
    Thread.sleep(1000);
    var messages = redisConnection.sync()
            .xreadgroup(Consumer.from("application_1", "consumer_1"),
                    XReadArgs.Builder.block(6000),
                    XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(messages.size()).isGreaterThan(0);
    var xlen = redisConnection.sync().xlen("zeebe:DEPLOYMENT");
    for (StreamMessage<String, byte[]> message : messages) {
      redisConnection.async().xack("zeebe:DEPLOYMENT",  "application_1",  message.getId());
    };

    // when: cleanupHasRun
    var delay = Duration.ofSeconds(3);

    // then: cleanup removed all messages except the last ones
    await().atMost(Duration.ofSeconds(5)).pollDelay(delay).pollInterval(Duration.ofMillis(500)).pollInSameThread()
            .untilAsserted(() -> assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isLessThan(xlen));

  }

  @Test
  public void shouldNotDeleteWhenConsumedButNotAcknowledged() throws Exception {
    // given: some consumed but never acknowledged messages
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process-1.bpmn").send().join();
    Thread.sleep(1000);
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process-2.bpmn").send().join();
    redisConnection.sync().xgroupCreate(XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_2", XGroupCreateArgs.Builder.mkstream());
    Thread.sleep(1000);
    var messages = redisConnection.sync()
            .xreadgroup(Consumer.from("application_2", "consumer_2"),
                    XReadArgs.Builder.block(6000),
                    XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(messages.size()).isGreaterThan(0);
    var xlen = redisConnection.sync().xlen("zeebe:DEPLOYMENT");

    // when: cleanupHasRun
    var delay = Duration.ofSeconds(3);

    // then: cleanup did not remove messages
    await().atMost(Duration.ofSeconds(5)).pollDelay(delay).pollInterval(Duration.ofMillis(500)).pollInSameThread()
            .untilAsserted(() -> assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isGreaterThanOrEqualTo(xlen));
  }

  @Test
  public void shouldNotDeleteWhenNotAcknowledgedByAllGroups() throws Exception {
    // given: messages consumed and acknowledged by only one of two consumer groups
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process-1.bpmn").send().join();
    Thread.sleep(1000);
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process-2.bpmn").send().join();
    redisConnection.sync().xgroupCreate(XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_3", XGroupCreateArgs.Builder.mkstream());
    redisConnection.sync().xgroupCreate(XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_4", XGroupCreateArgs.Builder.mkstream());
    Thread.sleep(1000);
    var messages = redisConnection.sync()
            .xreadgroup(Consumer.from("application_3", "consumer_3"),
                    XReadArgs.Builder.block(6000),
                    XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(messages.size()).isGreaterThan(0);
    for (StreamMessage<String, byte[]> message : messages) {
      redisConnection.async().xack("zeebe:DEPLOYMENT",  "application_3",  message.getId());
    };
    var xlen = redisConnection.sync().xlen("zeebe:DEPLOYMENT");

    messages = redisConnection.sync()
            .xreadgroup(Consumer.from("application_4", "consumer_4"),
                    XReadArgs.Builder.block(6000),
                    XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(messages.size()).isGreaterThan(0);

    // when: cleanupHasRun
    var delay = Duration.ofSeconds(3);

    // then: cleanup did not remove messages
    await().atMost(Duration.ofSeconds(5)).pollDelay(delay).pollInterval(Duration.ofMillis(500)).pollInSameThread()
            .untilAsserted(() -> assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isGreaterThanOrEqualTo(xlen));

  }
}
