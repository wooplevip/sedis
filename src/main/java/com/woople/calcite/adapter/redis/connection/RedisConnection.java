package com.woople.calcite.adapter.redis.connection;

import com.woople.calcite.adapter.redis.connection.redisson.RedissonHashCommands;

public interface RedisConnection {
    RedissonHashCommands hashCommands();
    void close();
}
