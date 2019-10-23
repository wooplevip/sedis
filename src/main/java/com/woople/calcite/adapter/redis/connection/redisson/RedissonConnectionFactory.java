package com.woople.calcite.adapter.redis.connection.redisson;

import com.woople.calcite.adapter.redis.connection.RedisClusterConnection;
import com.woople.calcite.adapter.redis.connection.RedisConnection;
import com.woople.calcite.adapter.redis.connection.RedisConnectionFactory;
import org.redisson.Redisson;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedissonConnectionFactory implements RedisConnectionFactory {
    private final static Logger logger = LoggerFactory.getLogger(RedissonConnectionFactory.class);

    private final Config redissonConfig;

    public RedissonConnectionFactory(Config redissonConfig) {
        this.redissonConfig = redissonConfig;
    }

    @Override
    public RedisConnection getConnection() {
        if (redissonConfig.isClusterConfig()){
            logger.info("Cluster mode");
            return new RedissonClusterConnection(Redisson.create(this.redissonConfig));
        }

        logger.info("Single mode");
        return new RedissonConnection(Redisson.create(this.redissonConfig));
    }
}
