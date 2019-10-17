package com.woople.calcite.adapter.redis.connection.redisson;

import com.woople.calcite.adapter.redis.connection.RedisHashCommands;


import java.util.Map;

public class RedissonHashCommands implements RedisHashCommands {
    private final RedissonConnection connection;

    public RedissonHashCommands(RedissonConnection connection) {
        this.connection = connection;
    }

    @Override
    public Map<Object, Object> hGetAll(String key) {
        return this.connection.getRedissonClient().getMap(key).readAllMap();
    }
}
