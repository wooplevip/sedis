package com.woople.calcite.adapter.redis.connection.redisson;

import com.woople.calcite.adapter.redis.connection.RedisConnection;
import com.woople.calcite.adapter.redis.connection.RedisKeyCommands;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;


public class RedissonConnection implements RedisConnection {
    private final RedissonClient redissonClient;

    public RedissonConnection(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    public void close() {
        this.redissonClient.shutdown();
    }

    @Override
    public RedissonHashCommands hashCommands() {
        return new RedissonHashCommands(this);
    }

    @Override
    public RedisKeyCommands keyCommands() {
        return new RedissonKeyCommands(this);
    }

    public RBatch getRBatch(){
        return this.redissonClient.createBatch();
    }

    public RedissonClient getRedissonClient() {
        return redissonClient;
    }
}
