package io.zeebe.redis;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.redis.exporter.ProtobufCodec;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ExporterTest {

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
  public void shouldExportEventsAsProtobuf() throws Exception {
    // given
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process.bpmn").send().join();
    Thread.sleep(1000);

    // when
    final var message = redisConnection.sync()
            .xrange("zeebe:DEPLOYMENT", Range.create("-", "+")).get(0);

    // then
    assertThat(message).isNotNull();
    final var messageValue = message.getBody().values().iterator().next();

    final var record = Schema.Record.parseFrom(messageValue);
    assertThat(record.getRecord().is(Schema.DeploymentRecord.class)).isTrue();

    final var deploymentRecord = record.getRecord().unpack(Schema.DeploymentRecord.class);
    final Schema.DeploymentRecord.Resource resource = deploymentRecord.getResources(0);
    assertThat(resource.getResourceName()).isEqualTo("process.bpmn");
  }

  @Test
  public void shouldSupportConsumerGroups() throws Exception {
    // given
    zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process.bpmn").send().join();
    redisConnection.sync().xgroupCreate(XReadArgs.StreamOffset.from("zeebe:DEPLOYMENT", "0-0"), "application_1",
            XGroupCreateArgs.Builder.mkstream());
    Thread.sleep(1000);

    // when
    var messages = redisConnection.sync()
            .xreadgroup(Consumer.from("application_1", "consumer_1"),
                    XReadArgs.Builder.block(6000),
                    XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));

    // then
    assertThat(messages.size()).isGreaterThan(0);
    for (StreamMessage<String, byte[]> message : messages) {
      final var messageValue = message.getBody().values().iterator().next();

      final var record = Schema.Record.parseFrom(messageValue);
      assertThat(record.getRecord().is(Schema.DeploymentRecord.class)).isTrue();

      redisConnection.async().xack("zeebe:DEPLOYMENT",  "application_1",  message.getId());
    };

    messages = redisConnection.sync()
            .xreadgroup(Consumer.from("application_1", "consumer_1"),
                    XReadArgs.StreamOffset.lastConsumed("zeebe:DEPLOYMENT"));
    assertThat(messages.size()).isEqualTo(0);
  }
}
