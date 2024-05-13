
using Io.Zeebe.Exporter.Proto;
using Io.Zeebe.Redis.Connect.Csharp;
using NLog.Config;
using NLog;
using NLog.Extensions.Logging;
using Xunit.Abstractions;
using zeeb_redis_connector_test.testcontainers;
using Zeebe.Client;

using static PleaseWait.Dsl;
using static PleaseWait.TimeUnit;

namespace zeeb_redis_connector_test
{
    [Collection("TestContainer Collection")]
    public class ImportRecordsTest : IAsyncLifetime
    {
        private ZeebeRedisContainer? _container;
        private IZeebeClient? _zeebeClient;
        private ZeebeRedis? _zeebeRedis;

        private readonly ITestOutputHelper output;

        private readonly List<DeploymentRecord> deploymentRecords = new();
        private readonly List<IncidentRecord> incidentRecords = new();
        private readonly List<JobRecord> jobRecords = new();
        private readonly List<JobBatchRecord> jobBatchRecords = new();
        private readonly List<MessageRecord> messageRecords = new();
        private readonly List<MessageStartEventSubscriptionRecord> messageStartEventSubscriptionRecords = new();
        private readonly List<MessageSubscriptionRecord> messageSubscriptionRecords = new();
        private readonly List<ProcessRecord> processRecords = new();
        private readonly List<ProcessEventRecord> processEventRecords = new();
        private readonly List<ProcessInstanceRecord> processInstanceRecords = new();
        private readonly List<ProcessInstanceCreationRecord> processInstanceCreationRecords = new();
        private readonly List<ProcessMessageSubscriptionRecord> processMessageSubscriptionRecords = new();
        private readonly List<TimerRecord> timerRecords = new();
        private readonly List<UserTaskRecord> userTaskRecords = new();
        private readonly List<VariableRecord> variableRecords = new();
        private readonly List<VariableDocumentRecord> variableDocumentRecords = new();

        public ImportRecordsTest(ITestOutputHelper output)
        {
            this.output = output;
            var target = new NLog.Targets.MethodCallTarget("Xunit", (evt, args) => output.WriteLine("[" + evt.Level + "] " + evt.FormattedMessage + evt.Exception?.ToString()));
            var config = new LoggingConfiguration();
            config.LoggingRules.Add(new LoggingRule("*", LogLevel.Trace, target));
            LogManager.Configuration = config;
        }

        public async Task InitializeAsync()
        {
            // start test containers
            _container = new ZeebeRedisContainer();
            await _container.StartAsync();

            // create zeebe client
            _zeebeClient = await _container.CreateClientAsync();

            // prepare Redis Listener
            // connection config see https://stackexchange.github.io/StackExchange.Redis/Configuration.html
            ZeebeRedisOptions options = new()
            {
                RedisConfigString = "localhost",
                RedisConsumerGroup = "csharp-consumer"
            };
            _zeebeRedis = new ZeebeRedis(options, new NLogLoggerFactory());

            _zeebeRedis
                .AddDeploymentListener(record => deploymentRecords.Add(record))
                .AddIncidentListener(record => incidentRecords.Add(record))
                .AddJobListener(record => jobRecords.Add(record))
                .AddJobBatchListener(record => jobBatchRecords.Add(record))
                .AddMessageStartEventSubscriptionListener(record => messageStartEventSubscriptionRecords.Add(record))
                .AddMessageSubscriptionListener(record => messageSubscriptionRecords.Add(record))
                .AddMessageListener(record => messageRecords.Add(record))
                .AddProcessListener(record => processRecords.Add(record))
                .AddProcessEventListener(record => processEventRecords.Add(record))
                .AddProcessInstanceListener(record => processInstanceRecords.Add(record))
                .AddProcessInstanceCreationListener(record => processInstanceCreationRecords.Add(record))
                .AddProcessMessageSubscriptionListener(record => processMessageSubscriptionRecords.Add(record))
                .AddTimerListener(record => timerRecords.Add(record))
                .AddUserTaskListener(record => userTaskRecords.Add(record))
                .AddVariableListener(record => variableRecords.Add(record))
                .AddVariableDocumentListener(record => variableDocumentRecords.Add(record));

            _ = Task.Run(() => _zeebeRedis.StartConsumeEvents());
        }

        public Task DisposeAsync()
        {
            _zeebeRedis.Dispose();
            _zeebeClient.Dispose();
            _container.DisposeAsync().Wait();
            return Task.CompletedTask;
        }


        [Fact]
        public async void TestReceiveRecords()
        {
            // given
            await _zeebeClient.NewDeployCommand()
                .AddResourceFile(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "resources", "process.bpmn"))
                .AddResourceFile(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "resources", "message-process.bpmn"))
                .Send();

            // when
            var processInstance = await _zeebeClient.NewCreateProcessInstanceCommand()
                .BpmnProcessId("process")
                .LatestVersion()
                .Variables("{\"key\":\"key-1\"}")
                .Send();
            await _zeebeClient.NewSetVariablesCommand(processInstance.ProcessInstanceKey).Variables("{\"y\":2}").Send();

            await _zeebeClient.NewPublishMessageCommand().MessageName("start").CorrelationKey("key-2").Send();
            await _zeebeClient.NewPublishMessageCommand()
                .MessageName("message")
                .CorrelationKey("key-1")
                .TimeToLive(TimeSpan.FromMinutes(1))
                .Send();

            var jobsResponse = await _zeebeClient.NewActivateJobsCommand().JobType("test")
                .MaxJobsToActivate(1).Timeout(TimeSpan.FromSeconds(5)).Send();
            jobsResponse.Jobs.ToList().ForEach(async job => await _zeebeClient.NewCompleteJobCommand(job.Key).Send());

            var errorResponse = await _zeebeClient.NewActivateJobsCommand().JobType("error")
                .MaxJobsToActivate(1).Timeout(TimeSpan.FromSeconds(5)).Send();
            errorResponse.Jobs.ToList().ForEach(async job => await _zeebeClient.NewFailCommand(job.Key)
                .Retries(0).ErrorMessage("Error").Send());

            // then check consumed messages
            Wait().AtMost(10, SECONDS).PollInterval(2, SECONDS).Until(() =>
            {
                // deployment
                Assert.True(deploymentRecords.Count >= 2);
                Assert.All(deploymentRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.Deployment, r.Metadata.ValueType);
                });
                var deploymentIntent = deploymentRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("CREATE", deploymentIntent);
                Assert.Contains("CREATED", deploymentIntent);

                // incident
                Assert.True(incidentRecords.Count >= 1);
                Assert.All(incidentRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.Incident, r.Metadata.ValueType);
                });
                Assert.Contains("CREATED", incidentRecords.Select(r => r.Metadata.Intent));

                // job batch
                Assert.True(jobBatchRecords.Count >= 2);
                Assert.All(jobBatchRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.JobBatch, r.Metadata.ValueType);
                });
                var jobBatchIntent = jobBatchRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("ACTIVATE", jobBatchIntent);
                Assert.Contains("ACTIVATED", jobBatchIntent);

                // job
                Assert.True(jobRecords.Count >= 2);
                Assert.All(jobRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.Job, r.Metadata.ValueType);
                });
                var jobIntent = jobRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("CREATED", jobIntent);
                Assert.Contains("COMPLETED", jobIntent);

                // message
                Assert.True(messageRecords.Count >= 2);
                Assert.All(messageRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.Message, r.Metadata.ValueType);
                });
                var messageIntent = messageRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("PUBLISH", messageIntent);
                Assert.Contains("PUBLISHED", messageIntent);

                // message start event subscription
                Assert.True(messageStartEventSubscriptionRecords.Count >= 1);
                Assert.All(messageStartEventSubscriptionRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.MessageStartEventSubscription, r.Metadata.ValueType);
                });
                Assert.Contains("CREATED", messageStartEventSubscriptionRecords.Select(r => r.Metadata.Intent));

                // message subscription
                Assert.True(messageSubscriptionRecords.Count >= 3);
                Assert.All(messageSubscriptionRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.MessageSubscription, r.Metadata.ValueType);
                });
                var messageSubscriptionIntent = messageSubscriptionRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("CREATED", messageSubscriptionIntent);
                Assert.Contains("CORRELATING", messageSubscriptionIntent);
                Assert.Contains("CORRELATED", messageSubscriptionIntent);

                // process event
                Assert.True(processEventRecords.Count >= 2);
                Assert.All(processEventRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.ProcessEvent, r.Metadata.ValueType);
                });
                var processEventIntent = processEventRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("TRIGGERING", processEventIntent);
                Assert.Contains("TRIGGERED", processEventIntent);

                // process instance creation
                Assert.True(processInstanceCreationRecords.Count >= 2);
                Assert.All(processInstanceCreationRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.ProcessInstanceCreation, r.Metadata.ValueType);
                });
                var processInstanceCreationIntent = processInstanceCreationRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("CREATE", processInstanceCreationIntent);
                Assert.Contains("CREATED", processInstanceCreationIntent);

                // process instance
                Assert.True(processInstanceRecords.Count >= 3);
                Assert.All(processInstanceRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.ProcessInstance, r.Metadata.ValueType);
                });
                var processInstanceIntent = processInstanceRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("ACTIVATE_ELEMENT", processInstanceIntent);
                Assert.Contains("ELEMENT_ACTIVATING", processInstanceIntent);
                Assert.Contains("ELEMENT_ACTIVATED", processInstanceIntent);

                // process message subrcription
                Assert.True(processMessageSubscriptionRecords.Count >= 2);
                Assert.All(processMessageSubscriptionRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.ProcessMessageSubscription, r.Metadata.ValueType);
                });
                var processMessageSubscriptionIntent = processMessageSubscriptionRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("CREATED", processMessageSubscriptionIntent);
                Assert.Contains("CORRELATED", processMessageSubscriptionIntent);

                // process
                Assert.True(processRecords.Count >= 2);
                Assert.All(processRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.Process, r.Metadata.ValueType);
                });
                Assert.Contains("CREATED", processRecords.Select(r => r.Metadata.Intent));

                // timer
                Assert.True(timerRecords.Count >= 2);
                Assert.All(timerRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.Timer, r.Metadata.ValueType);
                });
                Assert.Contains("CREATED", timerRecords.Select(r => r.Metadata.Intent));

                // variabe document
                Assert.True(variableDocumentRecords.Count >= 2);
                Assert.All(variableDocumentRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.VariableDocument, r.Metadata.ValueType);
                });
                var variableDocumentIntent = variableDocumentRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("UPDATE", variableDocumentIntent);
                Assert.Contains("UPDATED", variableDocumentIntent);

                // variable
                Assert.True(variableRecords.Count >= 2);
                Assert.All(variableRecords, r =>
                {
                    Assert.Equal(RecordMetadata.Types.ValueType.Variable, r.Metadata.ValueType);
                });
                Assert.Contains("CREATED", variableRecords.Select(r => r.Metadata.Intent));

                return true;
            });

        }

        [Fact]
        public async void TestReceiveUserTaskRecords()
        {
            // given
            await _zeebeClient.NewDeployCommand()
                .AddResourceFile(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "resources", "usertask-process.bpmn"))
                .Send();

            // when
            var processInstance = await _zeebeClient.NewCreateProcessInstanceCommand()
                .BpmnProcessId("user_task_process")
                .LatestVersion()
                .Send();

            // then check consumed messages
            Wait().AtMost(10, SECONDS).PollInterval(2, SECONDS).Until(() =>
            {
                Assert.True(userTaskRecords.Count >= 1);
                Assert.All(userTaskRecords, r =>
                {
                    Assert.Equal(processInstance.ProcessInstanceKey, r.ProcessInstanceKey);
                    Assert.Equal(RecordMetadata.Types.ValueType.UserTask, r.Metadata.ValueType);
                });
                var userTaskIntent = userTaskRecords.Select(r => r.Metadata.Intent);
                Assert.Contains("CREATING", userTaskIntent); 
                Assert.Contains("CREATED", userTaskIntent);

                var candidateGroup = userTaskRecords.Select(r => r.CandidateGroup.Single());
                Assert.Contains("testGroup", candidateGroup);

                return true;
            });
        }

    }
}