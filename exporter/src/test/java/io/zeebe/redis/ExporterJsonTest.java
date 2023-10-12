package io.zeebe.redis;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.redis.exporter.ExporterConfiguration;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ExporterJsonTest {

    private static final BpmnModelInstance WORKFLOW =
            Bpmn.createExecutableProcess("process")
                    .startEvent("start")
                    .sequenceFlowId("to-task")
                    .serviceTask("task", s -> s.zeebeJobType("test"))
                    .sequenceFlowId("to-end")
                    .endEvent("end")
                    .done();

    private static final ExporterConfiguration CONFIGURATION = new ExporterConfiguration();

    @Container
    public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withJsonFormat();

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> redisConnection;

    @BeforeEach
    public void init() {
        redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
        redisConnection = redisClient.connect();
    }

    @AfterEach
    public void cleanUp() {
        redisConnection.sync().xtrim("zeebe:DEPLOYMENT", 0);
        redisConnection.close();
        redisClient.shutdown();
    }


    @Test
    public void shouldExportEventsAsJson() throws Exception {
        // given
        zeebeContainer.getClient().newDeployResourceCommand().addProcessModel(WORKFLOW, "process.bpmn").send().join();
        Thread.sleep(1000);

        // when
        final var message = redisConnection.sync()
                .xrange("zeebe:DEPLOYMENT", Range.create("-", "+")).get(0);

        // then
        assertThat(message).isNotNull();

        final var jsonRecord = message.getBody().values().stream().findFirst().get();

        assertThat(jsonRecord)
                .startsWith("{")
                .endsWith("}")
                .contains("\"valueType\":\"DEPLOYMENT\"")
                .contains("\"recordType\":\"COMMAND\"")
                .contains("\"intent\":\"CREATE\"");
    }
}
