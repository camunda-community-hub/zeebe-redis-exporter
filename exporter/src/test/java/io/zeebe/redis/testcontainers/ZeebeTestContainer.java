package io.zeebe.redis.testcontainers;

import io.camunda.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeContainer;
import java.util.logging.Logger;
import org.testcontainers.utility.DockerImageName;

public class ZeebeTestContainer extends ZeebeContainer {

  private static final Logger LOGGER = Logger.getLogger("ZeebeTestContainer");

  private RedisContainer redisContainer;

  private ZeebeClient zeebeClient;

  protected ZeebeTestContainer(RedisContainer redisContainer) {
    super(DockerImageName.parse("ghcr.io/camunda-community-hub/zeebe-with-redis-exporter"));
    withExposedPorts(26500, 9600);
    withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none");
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

  public ZeebeTestContainer andUseMaxTTLInSeconds(long maxTimeToLiveInSeconds) {
    withEnv("ZEEBE_REDIS_MAX_TIME_TO_LIVE_IN_SECONDS", Long.toString(maxTimeToLiveInSeconds));
    return this;
  }

  public ZeebeTestContainer andUseMinTTLInSeconds(long minTimeToLiveInSeconds) {
    withEnv("ZEEBE_REDIS_MIN_TIME_TO_LIVE_IN_SECONDS", Long.toString(minTimeToLiveInSeconds));
    return this;
  }

  public static ZeebeTestContainer withCleanupCycleInSeconds(long cleanupCycleInSeconds) {
    ZeebeTestContainer container = withDefaultConfig();
    return container.andUseCleanupCycleInSeconds(cleanupCycleInSeconds);
  }

  public ZeebeTestContainer andUseCleanupCycleInSeconds(long cleanupCycleInSeconds) {
    withEnv("ZEEBE_REDIS_CLEANUP_CYCLE_IN_SECONDS", Long.toString(cleanupCycleInSeconds));
    return this;
  }

  public ZeebeTestContainer doDeleteAfterAcknowledge(boolean deleteAfterAcknowledge) {
    withEnv("ZEEBE_REDIS_DELETE_AFTER_ACKNOWLEDGE", Boolean.toString(deleteAfterAcknowledge));
    return this;
  }

  public ZeebeTestContainer andUseConsumerJobTimeoutInSeconds(long jobTimeoutInSeconds) {
    withEnv("ZEEBE_REDIS_CONSUMER_JOB_TIMEOUT_IN_SECONDS", Long.toString(jobTimeoutInSeconds));
    return this;
  }

  public ZeebeClient getClient() {
    if (zeebeClient == null) {
      zeebeClient =
          ZeebeClient.newClientBuilder()
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

  public void restartWithoutRedis() {
    super.doStart();
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
