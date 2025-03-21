package io.zeebe.redis.connect.java;

import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;

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
    if (redisClient != null)
      return new UniversalRedisConnection<>(redisClient.connect(protobufCodec));
    return new UniversalRedisConnection<>(redisClusterClient.connect(protobufCodec));
  }

  public boolean isCluster() {
    return redisClusterClient != null;
  }

  public RedisClusterClient getRedisClusterClient() {
    return redisClusterClient;
  }

  public void setStandardClusterOptions() {
    if (redisClusterClient != null) {
      redisClusterClient.setOptions(createStandardOptions());
    }
  }

  private static ClusterClientOptions createStandardOptions() {
    return ClusterClientOptions.builder()
        .autoReconnect(true)
        // dynamic adaptive refresh
        .topologyRefreshOptions(dynamicRefreshOptions())
        // filter out failed nodes from the topology
        .nodeFilter(
            it ->
                !(it.is(RedisClusterNode.NodeFlag.FAIL)
                    || it.is(RedisClusterNode.NodeFlag.EVENTUAL_FAIL)
                    || it.is(RedisClusterNode.NodeFlag.HANDSHAKE)
                    || it.is(RedisClusterNode.NodeFlag.NOADDR)))
        .validateClusterNodeMembership(true)
        .build();
  }

  private static ClusterTopologyRefreshOptions dynamicRefreshOptions() {
    return ClusterTopologyRefreshOptions.builder()
        .enableAllAdaptiveRefreshTriggers()
        .enablePeriodicRefresh()
        .dynamicRefreshSources(true)
        .build();
  }
}
