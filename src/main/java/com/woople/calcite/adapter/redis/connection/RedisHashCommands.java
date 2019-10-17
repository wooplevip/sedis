package com.woople.calcite.adapter.redis.connection;

import java.util.Map;

public interface RedisHashCommands {

    Map<Object, Object> hGetAll(String key);

}
