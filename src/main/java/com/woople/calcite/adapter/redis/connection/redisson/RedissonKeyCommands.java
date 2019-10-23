package com.woople.calcite.adapter.redis.connection.redisson;

import com.google.common.collect.Lists;
import com.woople.calcite.adapter.redis.connection.RedisKeyCommands;

import java.util.List;

public class RedissonKeyCommands implements RedisKeyCommands {
    private final RedissonConnection connection;

    public RedissonKeyCommands(RedissonConnection connection) {
        this.connection = connection;
    }

    @Override
    public List<String> keys(String pattern, int count) {
        Iterable<String> keys = this.connection.getRedissonClient().getKeys().getKeysByPattern(pattern, count);
        return Lists.newArrayList(keys);
    }

}
