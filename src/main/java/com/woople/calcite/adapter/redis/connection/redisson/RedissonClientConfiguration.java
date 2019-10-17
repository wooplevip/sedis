package com.woople.calcite.adapter.redis.connection.redisson;

import java.util.HashMap;
import java.util.Map;

public class RedissonClientConfiguration {
    public Map<String, String> getParam() {
        return param;
    }

    private final Map<String, String> param = new HashMap<>();
}
