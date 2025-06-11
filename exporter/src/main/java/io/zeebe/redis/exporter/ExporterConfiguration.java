package io.zeebe.redis.exporter;

import io.lettuce.core.RedisURI;
import java.time.Duration;
import java.util.Optional;

public class ExporterConfiguration {

  private static final String ENV_PREFIX = "ZEEBE_REDIS_";

  private String format = "protobuf";

  private String enabledValueTypes = "";
  private String enabledRecordTypes = "";

  private String name = "zeebe";

  private String remoteAddress;
  private boolean useClusterClient = false;

  private long cleanupCycleInSeconds = Duration.ofMinutes(1).toSeconds();

  private long minTimeToLiveInSeconds = 0;

  private long maxTimeToLiveInSeconds = Duration.ofMinutes(5).toSeconds();

  private boolean deleteAfterAcknowledge = false;

  private long consumerJobTimeoutInSeconds = Duration.ofMinutes(5).toSeconds();

  private long consumerIdleTimeoutInSeconds = Duration.ofHours(24).toSeconds();

  private int ioThreadPoolSize = Math.max(2, Runtime.getRuntime().availableProcessors());

  private int batchSize = 250;

  private int batchCycleMillis = 500;

  public long getCleanupCycleInSeconds() {
    return getEnv("CLEANUP_CYCLE_IN_SECONDS").map(Long::parseLong).orElse(cleanupCycleInSeconds);
  }

  public long getMinTimeToLiveInSeconds() {
    return getEnv("MIN_TIME_TO_LIVE_IN_SECONDS")
        .map(Long::parseLong)
        .orElse(minTimeToLiveInSeconds);
  }

  public long getMaxTimeToLiveInSeconds() {
    return getEnv("MAX_TIME_TO_LIVE_IN_SECONDS")
        .map(Long::parseLong)
        .orElse(maxTimeToLiveInSeconds);
  }

  public boolean isDeleteAfterAcknowledge() {
    return getEnv("DELETE_AFTER_ACKNOWLEDGE")
        .map(Boolean::parseBoolean)
        .orElse(deleteAfterAcknowledge);
  }

  public long getConsumerJobTimeoutInSeconds() {
    return getEnv("CONSUMER_JOB_TIMEOUT_IN_SECONDS")
        .map(Long::parseLong)
        .orElse(consumerJobTimeoutInSeconds);
  }

  public long getConsumerIdleTimeoutInSeconds() {
    return getEnv("CONSUMER_IDLE_TIMEOUT_IN_SECONDS")
        .map(Long::parseLong)
        .orElse(consumerIdleTimeoutInSeconds);
  }

  public int getIoThreadPoolSize() {
    return getEnv("IO_THREAD_POOL_SIZE").map(Integer::parseInt).orElse(ioThreadPoolSize);
  }

  public int getBatchSize() {
    return getEnv("BATCH_SIZE").map(Integer::parseInt).orElse(batchSize);
  }

  public int getBatchCycleMillis() {
    return getEnv("BATCH_CYCLE_MILLIS").map(Integer::parseInt).orElse(batchCycleMillis);
  }

  public String getFormat() {
    return getEnv("FORMAT").orElse(format);
  }

  public String getEnabledValueTypes() {
    return getEnv("ENABLED_VALUE_TYPES").orElse(enabledValueTypes);
  }

  public String getEnabledRecordTypes() {
    return getEnv("ENABLED_RECORD_TYPES").orElse(enabledRecordTypes);
  }

  public String getName() {
    return getEnv("NAME").orElse(name);
  }

  public Optional<RedisURI> getRemoteAddress() {
    return getEnv("REMOTE_ADDRESS")
        .or(() -> Optional.ofNullable(remoteAddress))
        .filter(remoteAddress -> !remoteAddress.isEmpty())
        .map(RedisURI::create);
  }

  public boolean isUseClusterClient() {
    return getEnv("USE_CLUSTER_CLIENT").map(Boolean::parseBoolean).orElse(useClusterClient);
  }

  private Optional<String> getEnv(String name) {
    return Optional.ofNullable(System.getenv(ENV_PREFIX + name));
  }

  @Override
  public String toString() {
    return "["
        + "remoteAddress='"
        + getRemoteAddress()
        + '\''
        + ", useClusterClient='"
        + isUseClusterClient()
        + '\''
        + ", enabledValueTypes='"
        + getEnabledValueTypes()
        + '\''
        + ", enabledRecordTypes='"
        + getEnabledRecordTypes()
        + '\''
        + ", format='"
        + getFormat()
        + '\''
        + ", name='"
        + getName()
        + '\''
        + ", cleanupCycleInSeconds="
        + getCleanupCycleInSeconds()
        + ", minTimeToLiveInSeconds="
        + getMinTimeToLiveInSeconds()
        + ", maxTimeToLiveInSeconds="
        + getMaxTimeToLiveInSeconds()
        + ", deleteAfterAcknowledge="
        + isDeleteAfterAcknowledge()
        + ", consumerJobTimeoutInSeconds="
        + getConsumerJobTimeoutInSeconds()
        + ", consumerIdleTimeoutInSeconds="
        + getConsumerIdleTimeoutInSeconds()
        + ", ioThreadPoolSize="
        + getIoThreadPoolSize()
        + ", batchSize="
        + getBatchSize()
        + ", batchCycleMillis="
        + getBatchCycleMillis()
        + ']';
  }
}
