package com.woople.calcite.adapter.redis.connection.redisson;

import com.woople.calcite.adapter.redis.connection.RedisClusterConnection;
import org.redisson.api.RedissonClient;

public class RedissonClusterConnection extends RedissonConnection implements RedisClusterConnection {

    public RedissonClusterConnection(RedissonClient redissonClient) {
        super(redissonClient);
    }
}
