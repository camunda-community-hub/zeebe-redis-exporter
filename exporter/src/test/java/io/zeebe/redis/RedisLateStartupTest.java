package io.zeebe.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.redis.testcontainers.OnFailureExtension;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
@ExtendWith(OnFailureExtension.class)
public class RedisLateStartupTest {

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
      ZeebeTestContainer.withJsonFormat()
          .andUseCleanupCycleInSeconds(2)
          .doDeleteAfterAcknowledge(true);

  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> redisConnection;

  @BeforeEach
  public void init() {
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
    redisConnection = redisClient.connect();
    redisConnection.sync().xtrim("zeebe:DEPLOYMENT", 0);
  }

  @AfterEach
  public void cleanUp() {
    redisConnection.sync().xtrim("zeebe:DEPLOYMENT", 0);
    redisConnection.close();
    redisClient.shutdown();
  }

  @Test
  public void worksCorrectIfRedisIsUnavailableAtStartup() throws Exception {
    // given
    redisConnection.close();
    redisClient.shutdown();
    zeebeContainer.stop();
    zeebeContainer.restartWithoutRedis();
    WaitingConsumer consumer = new WaitingConsumer();
    zeebeContainer.followOutput(consumer);
    consumer.waitUntil(
        frame -> frame.getUtf8String().contains("Broker is ready"), 30, TimeUnit.SECONDS);
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process.bpmn")
        .send()
        .join();
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process2.bpmn")
        .send()
        .join();
    consumer.waitUntil(
        frame -> frame.getUtf8String().contains("Failure connecting Redis exporter"),
        20,
        TimeUnit.SECONDS);

    // when
    zeebeContainer.getRedisContainer().start();
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
    redisConnection = redisClient.connect();
    consumer.waitUntil(
        frame -> frame.getUtf8String().contains("Successfully connected Redis exporter"),
        20,
        TimeUnit.SECONDS);

    Thread.sleep(1000);
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process3.bpmn")
        .send()
        .join();

    redisConnection
        .sync()
        .xgroupCreate(
            XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_1",
            XGroupCreateArgs.Builder.mkstream());

    // then
    AtomicReference<Long> xlen = new AtomicReference<>();
    Awaitility.await()
        .pollInSameThread()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              var messages =
                  redisConnection
                      .sync()
                      .xreadgroup(
                          Consumer.from("application_1", "consumer_1"),
                          XReadArgs.Builder.block(1000),
                          XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));

              long createdCount =
                  messages.stream()
                      .map(m -> m.getBody().values().stream().findFirst().get())
                      .filter(json -> json.contains("\"valueType\":\"DEPLOYMENT\""))
                      .filter(json -> json.contains("\"recordType\":\"EVENT\""))
                      .filter(json -> json.contains("\"intent\":\"CREATED\""))
                      .count();

              // assert that all messages have been received
              assertThat(createdCount).isEqualTo(3);

              // acknowledge all messages so that cleanup can work
              xlen.set(redisConnection.sync().xlen("zeebe:DEPLOYMENT"));
              for (StreamMessage<String, String> message : messages) {
                redisConnection.sync().xack("zeebe:DEPLOYMENT", "application_1", message.getId());
              }
              ;
            });

    // assert that cleanup still works and removed all messages except the last ones
    var delay = Duration.ofSeconds(3);
    await()
        .atMost(Duration.ofSeconds(7))
        .pollDelay(delay)
        .pollInterval(Duration.ofSeconds(1))
        .pollInSameThread()
        .untilAsserted(
            () ->
                assertThat(redisConnection.sync().xlen("zeebe:DEPLOYMENT")).isLessThan(xlen.get()));
  }
}
