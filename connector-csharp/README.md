C# Zeebe Redis Connector
========================

This library enables the retrieval of Zeebe events with C#. It is based on [StackExchange.Redis](https://www.nuget.org/packages/StackExchange.Redis/) 
and requires the configuration of the Zeebe Exporter as described in the main project.

Current limitations:
* Uses a Multi-key operation to receive events from Redis and thus does not yet work with Redis Clusters.
* StackExchange.Redis does not yet support blocking operations - hence this library is forced to use polling.
* It is - not yet - possible to configure a stream prefix other than the default. For now this is hardwired to `zeebe:`.

# Requirements

* [.NET 8.0](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)

# Usage

The Zeebe Redis Connector extension is available via nuget (https://www.nuget.org/packages/zeebe-redis-connector/).

## Bootstrap

The library provides an extension for hosted services `IServiceCollection.AddZeebeRedis(...)`
in order to bootstrap the connector. It requires `ZeebeRedisOptions`.

You can either wire your options manually
```
ConfigureServices((hostContext, services) => {
    services.AddZeebeRedis(options => {
        options.RedisConfigString = "localhost";
        options.RedisConsumerGroup = "my-consumer-group";
        options.RedisPollIntervallMillis = 500;
    })
    .AddSingleton<ZeebeRedisListener>();
})
```
or use a corresponding configuration section

```
ConfigureServices((hostContext, services) => {
    services.AddZeebeRedis(
        hostContext.Configuration.GetSection("ZeebeRedisConfiguration")
    })
    .AddSingleton<ZeebeRedisListener>();
})
```
with
```
{
  "ZeebeRedisConfiguration": {
    "RedisConfigString": "localhost",
    "RedisConsumerGroup": "my-csharp-consumer",
    "RedisPollIntervallMillis" : 500
  }
}
```


Lastly, it is also possible to use environment variables:

```
ConfigureServices((hostContext, services) => {
    services.AddZeebeRedis()
    .AddSingleton<ZeebeRedisListener>();
})
```

| Environment Parameter       | Description  |
|-----------------------------|---|
| REDIS_CONFIG_STRING         | Redis URL as required by [StackExchange.Redis](https://stackexchange.github.io/StackExchange.Redis/Configuration#configuration-options). Default: `localhost` |
| REDIS_CONSUMER_GROUP        | The consumer group. Default: empty |
| REDIS_POLL_INTERVALL_MILLIS | Poll interval in milliseconds. Default: `500`  |

Environment variables always have precedence over other ways of configuration.

**Hint:** It is strongly recommended to set the consumer group name. Otherwise, you will get a unique disposable group generated at startup.

## Registering Zeebe Event Listeners

By injecting `ZeebeRedis` in one of your services you're able to register listeners for specific events.
Registering listeners should happen before the startup of your application e.g. in the constructor.

```
public class ZeebeRedisListener
{
    public ZeebeRedisListener(ZeebeRedis zeebeRedis) {
        zeebeRedis
            .AddDeploymentListener((deployment) => ReceiveDeploymentRecord(deployment))
            .AddIncidentListener((incident) => ...)
            .AddJobListener((job) => ...)
            ...
            ;
    }

    private void ReceiveDeploymentRecord(DeploymentRecord deploymentRecord)
    {
        ...
    }
}
```