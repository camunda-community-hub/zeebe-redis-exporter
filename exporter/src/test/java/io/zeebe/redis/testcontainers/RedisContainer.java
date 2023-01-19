package io.zeebe.redis.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import static java.lang.String.format;

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
        withExposedPorts(PORT);
    }

    public String getRedisServerExternalAddress() {
        return this.getHost() + ":" + this.getMappedPort(PORT);
    }

    public String getRedisServerAddress() {
        return ALIAS + ":" + PORT;
    }
}
