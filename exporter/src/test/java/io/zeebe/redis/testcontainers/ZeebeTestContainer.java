package io.zeebe.redis.testcontainers;

import io.camunda.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

public class ZeebeTestContainer extends ZeebeContainer {

    private RedisContainer redisContainer;

    private ZeebeClient zeebeClient;

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

    public ZeebeTestContainer withMaxTTLInSeconds(long maxTimeToLiveInSeconds) {
        withEnv("ZEEBE_REDIS_MAX_TIME_TO_LIVE_IN_SECONDS", Long.toString(maxTimeToLiveInSeconds));
        return this;
    }

    public ZeebeTestContainer withMinTTLInSeconds(long minTimeToLiveInSeconds) {
        withEnv("ZEEBE_REDIS_MIN_TIME_TO_LIVE_IN_SECONDS", Long.toString(minTimeToLiveInSeconds));
        return this;
    }

    public static ZeebeTestContainer withCleanupCycleInSeconds(long cleanupCycleInSeconds) {
        ZeebeTestContainer container = withDefaultConfig();
        container.withEnv("ZEEBE_REDIS_CLEANUP_CYCLE_IN_SECONDS", Long.toString(cleanupCycleInSeconds));
        return container;
    }
    public ZeebeTestContainer doDeleteAfterAcknowledge(boolean deleteAfterAcknowledge) {
        withEnv("ZEEBE_REDIS_DELETE_AFTER_ACKNOWLEDGE", Boolean.toString(deleteAfterAcknowledge));
        return this;
    }

    public ZeebeClient getClient() {
        if (zeebeClient == null) {
            zeebeClient = ZeebeClient.newClientBuilder()
                    .gatewayAddress(getExternalGatewayAddress())
                    .usePlaintext()
                    .build();
        }
        return zeebeClient;
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
        if (zeebeClient != null) {
            zeebeClient.close();
        }
        zeebeClient = null;
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
