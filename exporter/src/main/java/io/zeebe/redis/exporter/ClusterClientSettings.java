package io.zeebe.redis.exporter;

import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.resource.ClientResources;

public class ClusterClientSettings {

    public static ClientResources createResourcesFromConfig(ExporterConfiguration config) {
        return ClientResources.builder()
                .ioThreadPoolSize(config.getIoThreadPoolSize())
                .build();
    }

    public static ClusterClientOptions createStandardOptions() {
        return ClusterClientOptions.builder()
            .autoReconnect(true)
            // dynamic adaptive refresh
            .topologyRefreshOptions(dynamicRefreshOptions())
            // filter out failed nodes from the topology
            .nodeFilter(it ->
                    ! (it.is(RedisClusterNode.NodeFlag.FAIL)
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
