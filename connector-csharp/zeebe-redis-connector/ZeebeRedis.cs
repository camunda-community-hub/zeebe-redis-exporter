using Io.Zeebe.Exporter.Proto;
using Io.Zeebe.Redis.Connect.Csharp.Consumer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Io.Zeebe.Redis.Connect.Csharp
{
    public class ZeebeRedis
    {
        private readonly ILogger? _logger;
        private CancellationTokenSource? _cancellationTokenSource = null;
        private readonly ConnectionMultiplexer _redisConnection;
        private readonly string _consumerGroup;
        private string _consumerId;
        private readonly bool _deleteConsumerGroupOnDispose = false;
        private readonly IDatabase _database;
        private readonly List<StreamPosition> _streamPositions = new List<StreamPosition>();
        private readonly Dictionary<RedisKey, IRecordConsumer> _consumer = new Dictionary<RedisKey, IRecordConsumer>();
        private readonly int _pollIntervalMillis;

        private readonly bool _closeRedisConnectionOnDispose = false;
        private bool _disposeCancellationTokenOnDispose = false;

        //---------------------------------------------------------------------
        // Constructors
        //---------------------------------------------------------------------

        public ZeebeRedis(ConnectionMultiplexer redisConnection, string? consumerGroup = null, ILoggerFactory? loggerFactory = null, int pollIntervalMillis = 500, bool closeRedisConnectionOnDispose = false)
        {
            _redisConnection = redisConnection ?? throw new ArgumentNullException(nameof(redisConnection));
            _consumerGroup = consumerGroup ?? Guid.NewGuid().ToString();
            _deleteConsumerGroupOnDispose = consumerGroup == null;
            _database = _redisConnection.GetDatabase();
            _logger = loggerFactory?.CreateLogger<ZeebeRedis>();
            _closeRedisConnectionOnDispose = closeRedisConnectionOnDispose;
            _pollIntervalMillis = pollIntervalMillis;

            redisConnection.ConnectionFailed += RedisConnection_ConnectionFailed;
            redisConnection.ConnectionRestored += RedisConnection_ConnectionRestored;

            _logger?.LogInformation("Using Redis connection configuration: {0}", redisConnection.Configuration);
        }

        public ZeebeRedis(ZeebeRedisOptions options, ILoggerFactory? loggerFactory = null) :
            this(CreateRedisConnectionFromConfig(options), options.RedisConsumerGroup, loggerFactory, options.RedisPollIntervallMillis, true) { }

        public ZeebeRedis(IOptions<ZeebeRedisOptions> options, ILoggerFactory? loggerFactory = null) :
            this(options.Value ?? throw new ArgumentNullException(nameof(options)), loggerFactory) { }

        private static ConnectionMultiplexer CreateRedisConnectionFromConfig(ZeebeRedisOptions redisOptions)
        {
            if (redisOptions == null) throw new ArgumentNullException(nameof(redisOptions));
            var config = ConfigurationOptions.Parse(redisOptions.RedisConfigString);
            if (!redisOptions.RedisConfigString.Contains("connectRetry="))
            {
                config.ConnectRetry = 120;
            }
            config.AbortOnConnectFail = false;
            config.ReconnectRetryPolicy = new LinearRetry(1000);
            return ConnectionMultiplexer.Connect(config);
        }

        //---------------------------------------------------------------------
        // Listeners
        //---------------------------------------------------------------------

        public ZeebeRedis AddAuthorizationListener(Action<AuthorizationRecord> listener)
        {
            var stream = AuthorizationRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new AuthorizationRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddClockListener(Action<ClockRecord> listener)
        {
            var stream = ClockRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ClockRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddCompensationSubscriptionListener(Action<CompensationSubscriptionRecord> listener)
        {
            var stream = CompensationSubscriptionRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new CompensationSubscriptionRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddDeploymentListener(Action<DeploymentRecord> listener)
        {
            var stream = DeploymentRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new DeploymentRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddDeploymentDistributionListener(Action<DeploymentDistributionRecord> listener)
        {
            var stream = DeploymentDistributionRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new DeploymentDistributionRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddErrorListener(Action<ErrorRecord> listener)
        {
            var stream = ErrorRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ErrorRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddEscalationListener(Action<EscalationRecord> listener)
        {
            var stream = EscalationRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new EscalationRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddFormListener(Action<FormRecord> listener)
        {
            var stream = FormRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new FormRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddGroupListener(Action<GroupRecord> listener)
        {
            var stream = GroupRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new  GroupRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddIdentitySetupListener(Action<IdentitySetupRecord> listener)
        {
            var stream = IdentitySetupRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new IdentitySetupRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddIncidentListener(Action<IncidentRecord> listener)
        {
            var stream = IncidentRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new IncidentRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddJobListener(Action<JobRecord> listener)
        {
            var stream = JobRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new JobRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddJobBatchListener(Action<JobBatchRecord> listener)
        {
            var stream = JobBatchRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new JobBatchRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddMappingRuleListener(Action<MappingRuleRecord> listener)
        {
            var stream = MappingRuleRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new MappingRuleRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddMessageBatchListener(Action<MessageBatchRecord> listener)
        {
            var stream = MessageBatchRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new MessageBatchRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddMessageCorrelationListener(Action<MessageCorrelationRecord> listener)
        {
            var stream = MessageCorrelationRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new MessageCorrelationRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddMessageStartEventSubscriptionListener(Action<MessageStartEventSubscriptionRecord> listener)
        {
            var stream = MessageStartEventSubscriptionRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new MessageStartEventSubscriptionRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddMessageSubscriptionListener(Action<MessageSubscriptionRecord> listener)
        {
            var stream = MessageSubscriptionRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new MessageSubscriptionRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddMessageListener(Action<MessageRecord> listener)
        {
            var stream = MessageRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new MessageRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddMultiInstanceListener(Action<MultiInstanceRecord> listener)
        {
            var stream = MultiInstanceRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new  MultiInstanceRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddProcessListener(Action<ProcessRecord> listener)
        {
            var stream = ProcessRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ProcessRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddProcessEventListener(Action<ProcessEventRecord> listener)
        {
            var stream = ProcessEventRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ProcessEventRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddProcessInstanceListener(Action<ProcessInstanceRecord> listener)
        {
            var stream = ProcessInstanceRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream,new ProcessInstanceRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddProcessInstanceBatchListener(Action<ProcessInstanceBatchRecord> listener)
        {
            var stream = ProcessInstanceBatchRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ProcessInstanceBatchRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddProcessInstanceCreationListener(Action<ProcessInstanceCreationRecord> listener)
        {
            var stream = ProcessInstanceCreationRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ProcessInstanceCreationRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddProcessInstanceMigrationListener(Action<ProcessInstanceMigrationRecord> listener)
        {
            var stream = ProcessInstanceMigrationRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ProcessInstanceMigrationRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddProcessInstanceResultListener(Action<ProcessInstanceResultRecord> listener)
        {
            var stream = ProcessInstanceResultRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ProcessInstanceResultRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddProcessMessageSubscriptionListener(Action<ProcessMessageSubscriptionRecord> listener)
        {
            var stream = ProcessMessageSubscriptionRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ProcessMessageSubscriptionRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddResourceDeletionListener(Action<ResourceDeletionRecord> listener)
        {
            var stream = ResourceDeletionRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ResourceDeletionRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddResourceListener(Action<ResourceRecord> listener)
        {
            var stream = ResourceRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new ResourceRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddRoleRecord(Action<RoleRecord> listener)
        {
            var stream = RoleRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new  RoleRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddTenantListener(Action<TenantRecord> listener)
        {
            var stream = TenantRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new TenantRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddTimerListener(Action<TimerRecord> listener)
        {
            var stream = TimerRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new TimerRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddUserTaskListener(Action<UserTaskRecord> listener)
        {
            var stream = UserTaskRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new UserTaskRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddUserListener(Action<UserRecord> listener)
        {
            var stream = UserRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new UserRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddVariableListener(Action<VariableRecord> listener)
        {
            var stream = VariableRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new VariableRecordConsumer(listener));
            return this;
        }

        public ZeebeRedis AddVariableDocumentListener(Action<VariableDocumentRecord> listener)
        {
            var stream = VariableDocumentRecordConsumer.STREAM;
            _streamPositions.Add(new StreamPosition(stream, ">"));
            _consumer.Add(stream, new VariableDocumentRecordConsumer(listener));
            return this;
        }

        //---------------------------------------------------------------------
        // Message Handling
        //---------------------------------------------------------------------

        public async Task StartConsumeEvents(CancellationTokenSource? cancellationTokenSource = null)
        {
            this._cancellationTokenSource = cancellationTokenSource ?? new CancellationTokenSource();
            this._disposeCancellationTokenOnDispose = cancellationTokenSource == null;

            _logger?.LogInformation("Start consuming Zeebe events from {0} [consumerGroup={1}]", _redisConnection.GetEndPoints(), _consumerGroup);
            var cancellationToken = _cancellationTokenSource.Token;
            _consumerId = Guid.NewGuid().ToString();

            // create consumer groups
            foreach (var item in _streamPositions)
            {
                if (!(await _database.KeyExistsAsync(item.Key)) ||
                    (await _database.StreamGroupInfoAsync(item.Key)).All(x => x.Name != _consumerGroup))
                {
                    await _database.StreamCreateConsumerGroupAsync(item.Key, _consumerGroup, "0-0", true);
                }
            }

            var streams = _streamPositions.ToArray();
            _logger?.LogDebug("Read from streams {0}", _consumer.Keys);

            var consumerGroupReadTask = Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var streamResults = await _database.StreamReadGroupAsync(streams, _consumerGroup, _consumerId, 500);
                    Parallel.ForEach(streamResults, result =>
                    {
                        if (result.Entries.Length != 0)
                        {
                            string? id = string.Empty;
                            result.Entries.ToList().ForEach(entry =>
                            {
                                id = entry.Id;
                                var record = Record.Parser.ParseFrom(entry.Values.First().Value);

#pragma warning disable CS8600, CS8602
                                _consumer.TryGetValue(result.Key, out IRecordConsumer consumer);
                                consumer.Consume(record);
#pragma warning restore CS8600, CS8602
                                if (!string.IsNullOrEmpty(id))
                                {
                                    _database.StreamAcknowledge(result.Key, _consumerGroup, id);
                                }
                            });
                        }
                    });
                    await Task.Delay(_pollIntervalMillis, cancellationToken);
                }
            });

            await Task.WhenAll(consumerGroupReadTask);
        }

        public async Task DisposeAsync()
        {
            _cancellationTokenSource?.Cancel();
            // delay disposing, since consuming events takes some time to close
            await Task.Delay(TimeSpan.FromMilliseconds(_pollIntervalMillis*2)).ContinueWith(t =>
            {
                _logger?.LogInformation("Stopping ZeebeRedis");
                // delete consumer group
                if (_deleteConsumerGroupOnDispose)
                {
                    _logger?.LogDebug("Delete disposable one-time consumer group {0}", _consumerGroup);
                    foreach (var item in _streamPositions)
                    {
                        _database.StreamDeleteConsumerGroup(item.Key, _consumerGroup);
                    }
                }
                else
                {
                    _logger?.LogDebug("Delete consumer id {0}", _consumerId);
                    foreach (var item in _streamPositions)
                    {
                        _database.StreamDeleteConsumer(item.Key, _consumerGroup, _consumerId);
                    }
                }
                _redisConnection.ConnectionFailed -= RedisConnection_ConnectionFailed;
                if (_disposeCancellationTokenOnDispose) _cancellationTokenSource?.Dispose();
                if (_closeRedisConnectionOnDispose) _redisConnection.Close();
            });
        }

        public void Dispose() {
            var disposeTask = DisposeAsync();
            disposeTask.Wait();
        }

        private void RedisConnection_ConnectionFailed(object? sender, ConnectionFailedEventArgs args)
        {
            _logger?.LogError("Connection failed: {0}", args.Exception?.Message);
        }
        private void RedisConnection_ConnectionRestored(object? sender, ConnectionFailedEventArgs args)
        {
            _logger?.LogInformation("Connection restored: {0}", args.Exception?.Message);
        }
    }
}