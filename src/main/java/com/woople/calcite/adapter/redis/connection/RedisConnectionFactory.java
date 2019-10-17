package com.woople.calcite.adapter.redis.connection;

public interface RedisConnectionFactory {
    RedisConnection getConnection();

    RedisClusterConnection getClusterConnection();
}
