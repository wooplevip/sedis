package com.woople.calcite.adapter.redis.connection;

import java.util.List;
import java.util.Map;

public interface RedisHashCommands {

    Map<String, String> hGetAll(String key);

    List<Map<String, String>> hGetAll(List<String> keys);

}
