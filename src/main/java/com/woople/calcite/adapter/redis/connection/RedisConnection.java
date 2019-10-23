package com.woople.calcite.adapter.redis.connection;

public interface RedisConnection {
    RedisHashCommands hashCommands();
    RedisKeyCommands keyCommands();
    void close();
}
