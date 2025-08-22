package io.zeebe.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.redis.exporter.ProtobufCodec;
import io.zeebe.redis.testcontainers.OnFailureExtension;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(OnFailureExtension.class)
public class ExporterFilterTest {

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
      (ZeebeTestContainer)
          ZeebeTestContainer.withDefaultConfig()
              .withEnv("ZEEBE_LOG_LEVEL", "debug")
              .withEnv("ZEEBE_REDIS_ENABLED_RECORD_TYPES", "EVENT")
              .withEnv("ZEEBE_REDIS_ENABLED_VALUE_TYPES", "JOB")
              .withEnv("ZEEBE_REDIS_ENABLED_INTENTS", "JobIntent=CREATED");

  private RedisClient redisClient;
  private StatefulRedisConnection<String, byte[]> redisConnection;

  @BeforeEach
  public void init() {
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
    redisConnection = redisClient.connect(new ProtobufCodec());
    redisConnection.sync().xtrim("zeebe:JOB", 0);
  }

  @AfterEach
  public void cleanUp() {
    redisConnection.sync().xtrim("zeebe:JOB", 0);
    redisConnection.close();
    redisClient.shutdown();
  }

  @Test
  public void shouldExportEventsWithJobIntent() throws Exception {
    // given
    zeebeContainer
        .getClient()
        .newDeployResourceCommand()
        .addProcessModel(WORKFLOW, "process.bpmn")
        .send()
        .join();
    zeebeContainer
        .getClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("process")
        .latestVersion()
        .send()
        .join();
    Thread.sleep(1000);

    // when
    final var message = redisConnection.sync().xrange("zeebe:JOB", Range.create("-", "+")).get(0);

    // then
    assertThat(message).isNotNull();
    final var messageValue = message.getBody().values().iterator().next();

    final var record = Schema.Record.parseFrom(messageValue);
    assertThat(record.getRecord().is(Schema.JobRecord.class)).isTrue();

    final var jobRecord = record.getRecord().unpack(Schema.JobRecord.class);
    assertEquals(jobRecord.getType(), "test");

    assertEquals(jobRecord.getMetadata().getRecordType().toString(), RecordType.EVENT.toString());
    assertEquals(jobRecord.getMetadata().getValueType().toString(), ValueType.JOB.toString());
    assertEquals(jobRecord.getMetadata().getIntent().toString(), JobIntent.CREATED.toString());
  }
}
