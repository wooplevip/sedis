package com.woople.calcite.adapter.redis.connection.redisson;

import com.woople.calcite.adapter.redis.connection.RedisClusterConnection;
import com.woople.calcite.adapter.redis.connection.RedisConnection;
import com.woople.calcite.adapter.redis.connection.RedisConnectionFactory;
import org.redisson.Redisson;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedissonConnectionFactory implements RedisConnectionFactory {
    private final static Logger logger = LoggerFactory.getLogger(RedissonConnectionFactory.class);
    private Config redissonConfig;

    private final RedissonClientConfiguration clientConfiguration;

    public RedissonConnectionFactory(RedissonClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        redissonConfig = new Config();
        redissonConfig.setCodec(new org.redisson.client.codec.StringCodec());
        Class c;
        try {
            c = Class.forName("org.redisson.client.codec.StringCodec");
            redissonConfig.setCodec((Codec)c.newInstance());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createClusterServerConfig(){
        this.redissonConfig.useClusterServers()
                .setScanInterval(5000)
                .setConnectTimeout(100000)
                .setTimeout(100000).addNodeAddress(clientConfiguration.getParam().get("sedis.redis.cluster.nodes").split(","));
    }

    private void createSingleServerConfig(){
        this.redissonConfig.useSingleServer().setConnectTimeout(100000)
                .setTimeout(100000).setAddress(clientConfiguration.getParam().get("sedis.redis.node"));
    }

    @Override
    public RedisConnection getConnection() {
        createSingleServerConfig();
        return new RedissonConnection(Redisson.create(redissonConfig));
    }

    @Override
    public RedisClusterConnection getClusterConnection() {
        createClusterServerConfig();
        return new RedissonClusterConnection(Redisson.create(redissonConfig));
    }
}
