package com.woople.calcite.adapter.redis.connection;

public interface ClusterNodeResourceProvider {

	<S> S getResourceForSpecificNode(RedisClusterNode node);

	void returnResourceForSpecificNode(RedisClusterNode node, Object resource);
}
