package io.zeebe.redis.connect.java;

import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;

public class UniversalRedisClient {

    private RedisClient redisClient = null;
    private RedisClusterClient redisClusterClient = null;

    public UniversalRedisClient(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    public UniversalRedisClient(RedisClusterClient redisClient) {
        this.redisClusterClient = redisClient;
    }

    public UniversalRedisConnection<String, byte[]> connect(ProtobufCodec protobufCodec) {
        if (redisClient != null) return new UniversalRedisConnection<>(redisClient.connect(protobufCodec));
        return new UniversalRedisConnection<>(redisClusterClient.connect(protobufCodec));
    }

    public boolean isCluster() {
        return redisClusterClient != null;
    }

    public RedisClusterClient getRedisClusterClient() {
        return redisClusterClient;
    }

}
