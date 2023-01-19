package io.zeebe.redis.testcontainers;

import io.camunda.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

public class ZeebeTestContainer extends ZeebeContainer {

    private RedisContainer redisContainer;

    protected ZeebeTestContainer(RedisContainer redisContainer) {
        super(DockerImageName.parse("ghcr.io/camunda-community-hub/zeebe-with-redis-exporter"));
        withExposedPorts(26500,9600);
        dependsOn(redisContainer);
        this.redisContainer = redisContainer;
    }

    public static ZeebeTestContainer withDefaultConfig() {
        return new ZeebeTestContainer(new RedisContainer());
    }

    public static ZeebeTestContainer withJsonFormat() {
        ZeebeTestContainer container = withDefaultConfig();
        container.withEnv("ZEEBE_REDIS_FORMAT", "json");
        return container;
    }

    public static ZeebeTestContainer withTTLInSeconds(long timeToLiveInSeconds) {
        ZeebeTestContainer container = withDefaultConfig();
        container.withEnv("ZEEBE_REDIS_TIME_TO_LIVE_IN_SECONDS", Long.toString(timeToLiveInSeconds));
        return container;
    }

    public ZeebeClient getClient() {
        return ZeebeClient.newClientBuilder()
                .gatewayAddress(getExternalGatewayAddress())
                .usePlaintext()
                .build();
    }

    @Override
    public void start() {
        if (redisContainer != null) {
            redisContainer.start();
            withNetwork(redisContainer.getNetwork());
            withEnv("ZEEBE_REDIS_REMOTE_ADDRESS", "redis://" + redisContainer.getRedisServerAddress());
        }
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        if (redisContainer != null) {
            redisContainer.stop();
        }
    }

    public RedisContainer getRedisContainer() {
        return redisContainer;
    }

    public String getRedisAddress() {
        return "redis://" + redisContainer.getRedisServerExternalAddress();
    }
}
