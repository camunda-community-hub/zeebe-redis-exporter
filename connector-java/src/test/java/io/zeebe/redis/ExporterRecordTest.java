package io.zeebe.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.record.intent.*;
import io.lettuce.core.RedisClient;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.redis.connect.java.ZeebeRedis;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ExporterRecordTest {

  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess("process")
          .startEvent("start")
          .parallelGateway("fork")
          .serviceTask("task", s -> s.zeebeJobType("test").zeebeInputExpression("key", "x"))
          .endEvent("end")
          .moveToNode("fork")
          .receiveTask("receive-task")
          .message(m -> m.name("message").zeebeCorrelationKeyExpression("key"))
          .boundaryEvent("timer", b -> b.timerWithDuration("PT1M"))
          .endEvent()
          .moveToNode("receive-task")
          .serviceTask("errorTask", s -> s.zeebeJobType("error"))
          .endEvent()
          .done();

  private static final BpmnModelInstance MESSAGE_PROCESS =
      Bpmn.createExecutableProcess("message-process")
          .startEvent()
          .message("start")
          .zeebeOutputExpression("x", "x")
          .endEvent()
          .done();

  private final List<Schema.DeploymentRecord> deploymentRecords = new ArrayList<>();
  private final List<Schema.IncidentRecord> incidentRecords = new ArrayList<>();
  private final List<Schema.JobBatchRecord> jobBatchRecords = new ArrayList<>();
  private final List<Schema.JobRecord> jobRecords = new ArrayList<>();
  private final List<Schema.MessageRecord> messageRecords = new ArrayList<>();
  private final List<Schema.MessageStartEventSubscriptionRecord>
      messageStartEventSubscriptionRecords = new ArrayList<>();
  private final List<Schema.MessageSubscriptionRecord> messageSubscriptionRecords =
      new ArrayList<>();
  private final List<Schema.ProcessEventRecord> processEventRecords = new ArrayList<>();
  private final List<Schema.ProcessInstanceCreationRecord> processInstanceCreationRecords =
      new ArrayList<>();
  private final List<Schema.ProcessInstanceRecord> processInstanceRecords = new ArrayList<>();
  private final List<Schema.ProcessMessageSubscriptionRecord> processMessageSubscriptionRecords =
      new ArrayList<>();
  private final List<Schema.ProcessRecord> processRecords = new ArrayList<>();
  private final List<Schema.TimerRecord> timerRecords = new ArrayList<>();
  private final List<Schema.VariableDocumentRecord> variableDocumentRecords = new ArrayList<>();
  private final List<Schema.VariableRecord> variableRecords = new ArrayList<>();

  private ZeebeClient client;
  private ZeebeRedis zeebeRedis;

  @Container public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withMaxTTLInSeconds(-1);

  private RedisClient redisClient;

  @BeforeEach
  public void init() {
    client = zeebeContainer.getClient();
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());

    zeebeRedis =
        ZeebeRedis.newBuilder(redisClient)
            .addDeploymentListener(deploymentRecords::add)
            .addIncidentListener(incidentRecords::add)
            .addJobBatchListener(jobBatchRecords::add)
            .addJobListener(jobRecords::add)
            .addMessageListener(messageRecords::add)
            .addMessageStartEventSubscriptionListener(messageStartEventSubscriptionRecords::add)
            .addMessageSubscriptionListener(messageSubscriptionRecords::add)
            .addProcessEventListener(processEventRecords::add)
            .addProcessInstanceCreationListener(processInstanceCreationRecords::add)
            .addProcessInstanceListener(processInstanceRecords::add)
            .addProcessMessageSubscriptionListener(processMessageSubscriptionRecords::add)
            .addProcessListener(processRecords::add)
            .addTimerListener(timerRecords::add)
            .addVariableDocumentListener(variableDocumentRecords::add)
            .addVariableListener(variableRecords::add)
            .build();
  }

  @AfterEach
  public void cleanUp() {
    zeebeRedis.close();
    redisClient.shutdown();
  }

  @Test
  public void shouldExportRecords() {
    // given
    client
        .newDeployResourceCommand()
        .addProcessModel(PROCESS, "process.bpmn")
        .addProcessModel(MESSAGE_PROCESS, "message-process.bpmn")
        .send()
        .join();

    // when
    final var processInstance =
        client
            .newCreateInstanceCommand()
            .bpmnProcessId("process")
            .latestVersion()
            .variables(Map.of("key", "key-1"))
            .send()
            .join();

    client
        .newSetVariablesCommand(processInstance.getProcessInstanceKey())
        .variables(Map.of("y", 2))
        .send()
        .join();

    client.newPublishMessageCommand().messageName("start").correlationKey("key-2").send().join();
    client
        .newPublishMessageCommand()
        .messageName("message")
        .correlationKey("key-1")
        .timeToLive(Duration.ofMinutes(1))
        .send()
        .join();

    final var jobsResponse =
        client.newActivateJobsCommand().jobType("test").maxJobsToActivate(1).send().join();
    jobsResponse.getJobs().forEach(job -> client.newCompleteCommand(job.getKey()).send().join());

    final var errorResponse =
        client.newActivateJobsCommand().jobType("error").maxJobsToActivate(1).send().join();
    errorResponse
        .getJobs()
        .forEach(
            job ->
                client.newFailCommand(job.getKey()).retries(0).errorMessage("Error").send().join());

    // then
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              assertThat(deploymentRecords)
                  .hasSizeGreaterThanOrEqualTo(2)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.DEPLOYMENT))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(DeploymentIntent.CREATE.name(), DeploymentIntent.CREATED.name());

              assertThat(incidentRecords)
                  .hasSizeGreaterThanOrEqualTo(1)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.INCIDENT))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(IncidentIntent.CREATED.name());

              assertThat(jobBatchRecords)
                  .hasSizeGreaterThanOrEqualTo(2)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.JOB_BATCH))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(JobBatchIntent.ACTIVATE.name(), JobBatchIntent.ACTIVATED.name());

              assertThat(jobRecords)
                  .hasSizeGreaterThanOrEqualTo(2)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.JOB))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(JobIntent.CREATED.name(), JobIntent.COMPLETED.name());

              assertThat(messageRecords)
                  .hasSizeGreaterThanOrEqualTo(2)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.MESSAGE))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(MessageIntent.PUBLISH.name(), MessageIntent.PUBLISHED.name());

              assertThat(messageStartEventSubscriptionRecords)
                  .hasSizeGreaterThanOrEqualTo(1)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(
                                  Schema.RecordMetadata.ValueType.MESSAGE_START_EVENT_SUBSCRIPTION))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(MessageStartEventSubscriptionIntent.CREATED.name());

              assertThat(messageSubscriptionRecords)
                  .hasSizeGreaterThanOrEqualTo(3)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.MESSAGE_SUBSCRIPTION))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(
                      MessageSubscriptionIntent.CREATED.name(),
                      MessageSubscriptionIntent.CORRELATING.name(),
                      MessageSubscriptionIntent.CORRELATED.name());

              assertThat(processEventRecords)
                  .hasSizeGreaterThanOrEqualTo(2)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.PROCESS_EVENT))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(
                      ProcessEventIntent.TRIGGERING.name(), ProcessEventIntent.TRIGGERED.name());

              assertThat(processInstanceCreationRecords)
                  .hasSizeGreaterThanOrEqualTo(2)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.PROCESS_INSTANCE_CREATION))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(
                      ProcessInstanceCreationIntent.CREATE.name(),
                      ProcessInstanceCreationIntent.CREATED.name());

              assertThat(processInstanceRecords)
                  .hasSizeGreaterThanOrEqualTo(3)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.PROCESS_INSTANCE))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(
                      ProcessInstanceIntent.ACTIVATE_ELEMENT.name(),
                      ProcessInstanceIntent.ELEMENT_ACTIVATING.name(),
                      ProcessInstanceIntent.ELEMENT_ACTIVATED.name());

              assertThat(processMessageSubscriptionRecords)
                  .hasSizeGreaterThanOrEqualTo(2)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(
                                  Schema.RecordMetadata.ValueType.PROCESS_MESSAGE_SUBSCRIPTION))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(
                      ProcessMessageSubscriptionIntent.CREATED.name(),
                      ProcessMessageSubscriptionIntent.CORRELATED.name());

              assertThat(processRecords)
                  .hasSizeGreaterThanOrEqualTo(2)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.PROCESS))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(ProcessIntent.CREATED.name());

              assertThat(timerRecords)
                  .hasSizeGreaterThanOrEqualTo(1)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.TIMER))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(TimerIntent.CREATED.name());

              assertThat(variableDocumentRecords)
                  .hasSizeGreaterThanOrEqualTo(1)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.VARIABLE_DOCUMENT))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(
                      VariableDocumentIntent.UPDATE.name(), VariableDocumentIntent.UPDATED.name());

              assertThat(variableRecords)
                  .hasSizeGreaterThanOrEqualTo(2)
                  .allSatisfy(
                      r ->
                          assertThat(r.getMetadata().getValueType())
                              .isEqualTo(Schema.RecordMetadata.ValueType.VARIABLE))
                  .extracting(r -> r.getMetadata().getIntent())
                  .contains(VariableIntent.CREATED.name());
            });
  }
}
