package io.zeebe.redis;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.redis.testcontainers.OnFailureExtension;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class RedisUnavailabilityTest {

  private static final BpmnModelInstance WORKFLOW =
          Bpmn.createExecutableProcess("process")
                  .startEvent("start")
                  .sequenceFlowId("to-task")
                  .serviceTask("task", s -> s.zeebeJobType("test"))
                  .sequenceFlowId("to-end")
                  .endEvent("end")
                  .done();
  @Container
  public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withJsonFormat();

  @RegisterExtension
  static OnFailureExtension onFailureExtension = new OnFailureExtension();

  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> redisConnection;

  @BeforeEach
  public void init() {
    onFailureExtension.setZeebeTestContainer(zeebeContainer);
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
    redisConnection.close();
    redisClient.shutdown();
    zeebeContainer.getRedisContainer().stop();
    Thread.sleep(2000);
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process.bpmn").send().join();
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process2.bpmn").send().join();
    Thread.sleep(1000);

    // when
    zeebeContainer.getRedisContainer().start();
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
    redisConnection = redisClient.connect();
    Thread.sleep(5000);
    redisConnection.sync().xgroupCreate(XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_1", XGroupCreateArgs.Builder.mkstream());

    // then
    Awaitility.await().pollInterval(Duration.ofSeconds(1))
            .atMost(Duration.ofSeconds(10)).untilAsserted(() -> {

      var messages = redisConnection.sync()
              .xreadgroup(Consumer.from("application_1", "consumer_1"),
                      XReadArgs.Builder.block(1000),
                      XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));

      long createdCount = messages.stream().map(m -> m.getBody().values().stream().findFirst().get())
              .filter(json -> json.contains("\"valueType\":\"DEPLOYMENT\""))
              .filter(json -> json.contains("\"recordType\":\"EVENT\""))
              .filter(json -> json.contains("\"intent\":\"CREATED\""))
              .count();

      assertThat(createdCount).isEqualTo(2);
    });

  }
}
