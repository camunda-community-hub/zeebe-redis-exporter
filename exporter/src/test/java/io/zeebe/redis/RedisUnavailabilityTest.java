package io.zeebe.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.redis.testcontainers.OnFailureExtension;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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
public class RedisUnavailabilityTest {

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process")
          .startEvent("start")
          .sequenceFlowId("to-task")
          .serviceTask("task", s -> s.zeebeJobType("test"))
          .sequenceFlowId("to-end")
          .endEvent("end")
          .done();
  @Container public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withJsonFormat();

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
  public void worksCorrectIfRedisIsTemporarilyUnavailable() throws Exception {
    // given
    WaitingConsumer consumer = new WaitingConsumer();
    zeebeContainer.followOutput(consumer);
    consumer.waitUntil(
        frame -> frame.getUtf8String().contains("Successfully connected Redis exporter"),
        10,
        TimeUnit.SECONDS);

    redisConnection.close();
    redisClient.shutdown();
    zeebeContainer.getRedisContainer().stop();
    consumer.waitUntil(
        frame -> frame.getUtf8String().contains("Cannot reconnect to [redis"),
        10,
        TimeUnit.SECONDS);

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
    Thread.sleep(1000);

    // when
    zeebeContainer.getRedisContainer().start();
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
    redisConnection = redisClient.connect();
    consumer.waitUntil(
        frame -> frame.getUtf8String().contains("Redis connection re-established"),
        20,
        TimeUnit.SECONDS);
    redisConnection
        .sync()
        .xgroupCreate(
            XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_1",
            XGroupCreateArgs.Builder.mkstream());

    // then
    Awaitility.await()
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

              assertThat(createdCount).isEqualTo(2);
            });
  }
}
