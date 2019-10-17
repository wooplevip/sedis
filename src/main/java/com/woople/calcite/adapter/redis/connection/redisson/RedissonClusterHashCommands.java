package com.woople.calcite.adapter.redis.connection.redisson;

import com.woople.calcite.adapter.redis.connection.RedisHashCommands;

import java.util.Map;

public class RedissonClusterHashCommands extends RedissonHashCommands {
    public RedissonClusterHashCommands(RedissonConnection connection) {
        super(connection);
    }
}
