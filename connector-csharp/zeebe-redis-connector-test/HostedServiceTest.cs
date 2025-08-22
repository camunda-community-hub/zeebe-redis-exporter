using NLog.Config;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;
using zeeb_redis_connector_test.testcontainers;
using Zeebe.Client;
using Microsoft.Extensions.Hosting;
using Io.Zeebe.Exporter.Proto;
using Microsoft.Extensions.DependencyInjection;
using Io.Zeebe.Redis.Connect.Csharp;
using Io.Zeebe.Redis.Connect.Csharp.Hosting;
using NLog.Extensions.Hosting;

using static PleaseWait.Dsl;
using static PleaseWait.TimeUnit;
using Microsoft.Extensions.Logging;
using LogLevel = NLog.LogLevel;

namespace zeeb_redis_connector_test
{
    [Collection("TestContainer Collection")]
    public class HostedServiceTest : IAsyncLifetime
    {
        private ZeebeRedisContainer? _container;
        private IZeebeClient? _zeebeClient;

        public HostedServiceTest(ITestOutputHelper output)
        {
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
        }

        public Task DisposeAsync()
        {
            _zeebeClient.Dispose();
            _container.DisposeAsync().Wait();
            return Task.CompletedTask;
        }

        [Fact]
        public async void TestHostedService()
        {
            // given
            var host = Host.CreateDefaultBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddZeebeRedis(options => {
                        options.RedisConfigString = "localhost";
                        options.RedisPollIntervallMillis = 250;
                    })
                    .AddSingleton<ZeebeRedisListener>();
                })
                .UseNLog()
                .UseEnvironment("xUnit")
                .Build();

            var zeebeRedisListener = host.Services.GetService<ZeebeRedisListener>();
            var tokenSource = new CancellationTokenSource();
            _ = host.RunAsync(tokenSource.Token);

            // when
            await _zeebeClient.NewDeployCommand()
                .AddResourceFile(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "resources", "process.bpmn"))
                .AddResourceFile(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "resources", "message-process.bpmn"))
                .Send();

            // then
            try
            {
                // check consumed messages
                Wait().AtMost(10, Seconds).PollInterval(2, Seconds).Until(() =>
                {
                    // deployment
                    Assert.True(zeebeRedisListener.deploymentRecords.Count >= 2);
                    Assert.All(zeebeRedisListener.deploymentRecords, r =>
                    {
                        Assert.Equal(RecordMetadata.Types.ValueType.Deployment, r.Metadata.ValueType);
                    });
                    var deploymentIntent = zeebeRedisListener.deploymentRecords.Select(r => r.Metadata.Intent);
                    Assert.Contains("CREATE", deploymentIntent);
                    Assert.Contains("CREATED", deploymentIntent);

                    return true;
                });
            }
            finally {
                await host.StopAsync(tokenSource.Token);
                host.Dispose();
            }

        }

        private class ZeebeRedisListener
        {
            public volatile List<DeploymentRecord> deploymentRecords = new();
            private readonly ILogger<ZeebeRedisListener>? _logger = null;

            public ZeebeRedisListener(ZeebeRedis zeebeRedis, ILoggerFactory? loggerFactory = null) {
                _logger = loggerFactory?.CreateLogger<ZeebeRedisListener>();

                zeebeRedis.AddDeploymentListener((record) => ReceiveDeploymentRecord(record));
            }

            private void ReceiveDeploymentRecord(DeploymentRecord deploymentRecord)
            {
                _logger?.LogDebug("Received {0}", deploymentRecord);
                deploymentRecords.Add(deploymentRecord);
            }
        }
    }

}
