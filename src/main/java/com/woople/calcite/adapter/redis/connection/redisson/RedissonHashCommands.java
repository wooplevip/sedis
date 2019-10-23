package com.woople.calcite.adapter.redis.connection.redisson;

import com.woople.calcite.adapter.redis.connection.RedisHashCommands;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RMap;
import org.redisson.api.RMapAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.function.BiConsumer;

public class RedissonHashCommands implements RedisHashCommands {
    private final static Logger logger = LoggerFactory.getLogger(RedissonHashCommands.class);
    private final RedissonConnection connection;

    public RedissonHashCommands(RedissonConnection connection) {
        this.connection = connection;
    }

    @Override
    public Map<String, String> hGetAll(String key) {
        RMap<String, String> rMap = this.connection.getRedissonClient().getMap(key);
        return rMap.readAllMap();
    }

    @Override
    public List<Map<String, String>> hGetAll(List<String> keys) {
        List<Map<String, String>> result = new ArrayList<>();
        RBatch rBatch = this.connection.getRBatch();
        LinkedHashMap<String, RFuture<Map<String, String>>> rf = new LinkedHashMap<>();
        for (String key : keys) {
            RMapAsync<String, String> rMapAsync = rBatch.getMap(key);
            RFuture<Map<String, String>> rfMap = rMapAsync.readAllMapAsync();
            rf.put(key, rfMap);
        }

        for (String key : rf.keySet()) {
            RFuture<Map<String, String>> bazFuture = rf.get(key);
            bazFuture.whenComplete(new BiConsumer<Map<String, String>, Throwable>() {
                @Override
                public void accept(Map<String, String> objectObjectMap, Throwable throwable) {
                    result.add(objectObjectMap);
                }
            });
        }

        rBatch.execute();

        return result;
    }
}
