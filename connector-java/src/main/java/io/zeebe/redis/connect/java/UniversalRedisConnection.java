package io.zeebe.redis.connect.java;

import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.BoundedAsyncPool;

public class UniversalRedisConnection<K, V> {

  private StatefulRedisConnection<K, V> redisConnection = null;

  private StatefulRedisClusterConnection<K, V> redisClusterConnection = null;

  private StatefulConnection theConnection;

  public UniversalRedisConnection(StatefulConnection<K, V> redisConnection) {
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

  public RedisStreamAsyncCommands<K, V> asyncStreamCommands() {
    if (redisConnection != null) return redisConnection.async();
    return redisClusterConnection.async();
  }

  public void close() {
    theConnection.close();
  }

  public void releaseFromPool(BoundedAsyncPool<StatefulRedisClusterConnection<K, V>> redisPool) {
    if (redisPool != null && redisClusterConnection != null) {
      redisPool.release(redisClusterConnection);
    }
  }
}
