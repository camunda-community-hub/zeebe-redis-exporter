package io.zeebe.redis.exporter;

import java.time.Duration;
import java.util.Optional;

public class ExporterConfiguration {

  private static final String ENV_PREFIX = "ZEEBE_REDIS_";

  private long timeToLiveInSeconds = Duration.ofMinutes(5).toSeconds();

  private String format = "protobuf";

  private String enabledValueTypes = "";
  private String enabledRecordTypes = "";

  private String name = "zeebe";

  private String remoteAddress;

  public long getTimeToLiveInSeconds() {
    return getEnv("TIME_TO_LIVE_IN_SECONDS").map(Long::parseLong).orElse(timeToLiveInSeconds);
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

  public Optional<String> getRemoteAddress() {
    return getEnv("REMOTE_ADDRESS")
            .or(() -> Optional.ofNullable(remoteAddress))
            .filter(remoteAddress -> !remoteAddress.isEmpty());
  }

  private Optional<String> getEnv(String name) {
    return Optional.ofNullable(System.getenv(ENV_PREFIX + name));
  }

  @Override
  public String toString() {
    return "[remoteAddress="
        + getRemoteAddress()
        + ", enabledValueTypes="
        + getEnabledValueTypes()
        + ", enabledRecordTypes="
        + getEnabledRecordTypes()
        + ", timeToLiveInSeconds="
        + getTimeToLiveInSeconds()
        + ", format="
        + getFormat()
        + ", name="
        + getName()
        + "]";
  }
}
