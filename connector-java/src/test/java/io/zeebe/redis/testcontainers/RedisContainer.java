package io.zeebe.redis.testcontainers;

import static java.lang.String.format;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class RedisContainer extends GenericContainer<RedisContainer> {

  private static final String IMAGE_AND_VERSION_FORMAT = "%s:%s";
  public static String REDIS_DOCKER_IMAGE_NAME = "redis";
  public static String VERSION = "7-alpine";

  public static Integer PORT = 6379;
  public static String ALIAS = "redis";

  protected RedisContainer() {
    super(format(IMAGE_AND_VERSION_FORMAT, REDIS_DOCKER_IMAGE_NAME, VERSION));
  }

  @Override
  protected void configure() {
    withNetwork(Network.SHARED);
    withNetworkAliases(ALIAS);
    addFixedExposedPort(PORT, PORT);
  }

  public String getRedisServerExternalAddress() {
    return this.getHost() + ":" + PORT;
  }

  public String getRedisServerAddress() {
    return ALIAS + ":" + PORT;
  }
}
