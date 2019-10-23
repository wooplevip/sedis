package com.woople.calcite.adapter.redis.connection;

import java.util.List;
import java.util.Set;

public interface RedisKeyCommands {
    List<String> keys(String pattern, int count);
    default List<String> keys(String pattern){
        return keys(pattern, 5);
    }
}
