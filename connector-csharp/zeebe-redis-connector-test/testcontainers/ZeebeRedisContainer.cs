using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zeebe.Client;

namespace zeeb_redis_connector_test.testcontainers
{
    public class ZeebeRedisContainer
    {
        private readonly IContainer _redisContainer;

        private readonly IContainer _zeebeContainer;

        private readonly INetwork _network;

        public ZeebeRedisContainer() {
            // shared network
            _network = new NetworkBuilder().WithName("redis-test").Build();

            // Setup Redis
            _redisContainer = new ContainerBuilder()
                .WithImage("redis:7-alpine")
                .WithName("redis")
                .WithNetwork(_network)
                .WithNetworkAliases("redis")
                .WithPortBinding(6379)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(6379))
                .WithCleanUp(true)
                .Build();

            // Setup Zeebe with Redis Exporter
            _zeebeContainer = new ContainerBuilder()
                .WithImage("ghcr.io/camunda-community-hub/zeebe-with-redis-exporter")
                .WithName("zeebe-testcontainer")
                .WithNetwork(_network)
                .WithPortBinding(26500)
                .WithEnvironment("ZEEBE_REDIS_REMOTE_ADDRESS", "redis://redis")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(26500))
                .WithCleanUp(true)
                .Build();
        }

        public async Task<ZeebeRedisContainer> StartAsync()
        {
            await _redisContainer.StartAsync();
            await _zeebeContainer.StartAsync();
            return this;
        }

        public async Task DisposeAsync() {
            await _zeebeContainer.DisposeAsync();
            await _redisContainer.DisposeAsync();
            await _network.DeleteAsync();
            await _network.DisposeAsync();
        }

        public async Task<IZeebeClient> CreateClientAsync()
        {
            _ = Wait.ForUnixContainer().UntilPortIsAvailable(_zeebeContainer.GetMappedPublicPort(26500));

            // create zeebe client
            var zeebeClient = ZeebeClient.Builder()
                .UseGatewayAddress("127.0.0.1:" + _zeebeContainer.GetMappedPublicPort(26500))
                .UsePlainText()
                .Build();

            await WaitUntilBrokerIsReady(zeebeClient);

            return zeebeClient;
        }

        private async Task WaitUntilBrokerIsReady(IZeebeClient client)
        {
            var ready = false;
            do
            {
                try
                {
                    var topology = await client.TopologyRequest().Send();
                    ready = topology.Brokers[0].Partitions.Count == 1;
                }
                catch (Exception)
                {
                    Thread.Sleep(1000);
                }
            }
            while (!ready);
        }
    }
}
