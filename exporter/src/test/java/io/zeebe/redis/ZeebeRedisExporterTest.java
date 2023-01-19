package io.zeebe.redis;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
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
public class ZeebeRedisExporterTest {

  private static final BpmnModelInstance WORKFLOW =
          Bpmn.createExecutableProcess("process")
                  .startEvent("start")
                  .sequenceFlowId("to-task")
                  .serviceTask("task", s -> s.zeebeJobType("test"))
                  .sequenceFlowId("to-end")
                  .endEvent("end")
                  .done();
  @Container
  public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withDefaultConfig();

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
    redisConnection = redisClient.connect(new ProtobufCodec());
    Thread.sleep(5000);
    redisConnection.sync().xgroupCreate(XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"),
            "application_1", XGroupCreateArgs.Builder.mkstream());
    var messages = redisConnection.sync()
            .xreadgroup(Consumer.from("application_1", "consumer_1"),
                    XReadArgs.Builder.block(6000),
                    XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));

    // then
    assertThat(messages.size()).isEqualTo(6);

  }
}
