package io.zeebe.redis.exporter;

import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public class UniversalRedisConnection<K, V> {

    private StatefulRedisConnection<K, V> redisConnection = null;

    private StatefulRedisClusterConnection<K, V> redisClusterConnection = null;

    private StatefulConnection theConnection;
    public UniversalRedisConnection(StatefulConnection<K,V> redisConnection) {
        this.theConnection = redisConnection;
        if (redisConnection instanceof StatefulRedisConnection) {
            this.redisConnection = (StatefulRedisConnection<K, V>) redisConnection;
        } else {
            this.redisClusterConnection = (StatefulRedisClusterConnection<K, V>) redisConnection;
        }
    }

    public void addListener(RedisConnectionStateListener listener) {
        theConnection.addListener(listener);
    }

    public RedisStreamCommands<K, V> syncStreamCommands() {
        if (redisConnection != null) return redisConnection.sync();
        return redisClusterConnection.sync();
    }

    public RedisStringCommands<K, V> syncStringCommands() {
        if (redisConnection != null) return redisConnection.sync();
        return redisClusterConnection.sync();
    }

    public RedisStreamAsyncCommands<K, V> asyncStreamCommands() {
        if (redisConnection != null) return redisConnection.async();
        return redisClusterConnection.async();
    }

    public void setAutoFlushCommands(boolean autoFlush) {
        theConnection.setAutoFlushCommands(autoFlush);
    }

    public void flushCommands() {
        theConnection.flushCommands();
    }

    public void syncDel(K... ks) {
        if (redisConnection != null) {
            redisConnection.sync().del(ks);
        } else {
            redisClusterConnection.sync().del(ks);
        }
    }

    public void close() {
        theConnection.close();
    }
}
