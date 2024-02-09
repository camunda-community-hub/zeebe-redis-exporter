package io.zeebe.redis.exporter;

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

    public UniversalRedisConnection<String,?> connect(ProtobufCodec protobufCodec) {
        if (redisClient != null) return new UniversalRedisConnection<>(redisClient.connect(protobufCodec));
        return new UniversalRedisConnection<>(redisClusterClient.connect(protobufCodec));
    }

    public UniversalRedisConnection<String,?> connect() {
        if (redisClient != null) return new UniversalRedisConnection<>(redisClient.connect());
        return new UniversalRedisConnection<>(redisClusterClient.connect());
    }

    public void shutdown() {
        if (redisClient != null) {
            redisClient.shutdown();
        } else {
            redisClusterClient.shutdown();
        }
    }
}
