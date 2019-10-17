package com.woople.calcite.adapter.redis.connection;

public interface RedisCommands extends RedisHashCommands{
    default Object execute(String command, String... args){return new Object();};
}
