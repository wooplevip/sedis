package com.woople.calcite.adapter.redis.connection.redisson;


import com.woople.calcite.adapter.redis.connection.RedisConnectionFactory;
import org.junit.Test;

import java.util.Map;

public class RedissonConnectionTest {

    @Test
    public void testClusterCommand(){
        RedissonClientConfiguration conf = new RedissonClientConfiguration();
        conf.getParam().put("sedis.redis.cluster.nodes", "redis://10.1.236.179:6379,redis://10.1.236.179:6380,redis://10.1.236.179:6381");
        RedisConnectionFactory redisConnectionFactory = new RedissonConnectionFactory(conf);

        Map<Object, Object> result = redisConnectionFactory.getClusterConnection().hashCommands().hGetAll("baz");

        redisConnectionFactory.getClusterConnection().close();
        System.out.println(result);

    }

    @Test
    public void testSingleServerCommand(){
        RedissonClientConfiguration conf = new RedissonClientConfiguration();
        conf.getParam().put("sedis.redis.node", "redis://10.1.236.179:6379");
        RedisConnectionFactory redisConnectionFactory = new RedissonConnectionFactory(conf);

        Map<Object, Object> result = redisConnectionFactory.getConnection().hashCommands().hGetAll("baz");

        redisConnectionFactory.getConnection().close();
        System.out.println(result);
    }

}