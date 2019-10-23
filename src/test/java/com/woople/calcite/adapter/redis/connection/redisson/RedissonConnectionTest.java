package com.woople.calcite.adapter.redis.connection.redisson;


import com.woople.calcite.adapter.redis.connection.RedisClusterConnection;
import com.woople.calcite.adapter.redis.connection.RedisConnection;
import com.woople.calcite.adapter.redis.connection.RedisConnectionFactory;
import org.junit.Before;
import org.junit.Test;
import org.redisson.client.codec.Codec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.util.List;
import java.util.Map;

public class RedissonConnectionTest {
    Config config = new Config();
    @Before
    public void init(){

        config.setCodec(new org.redisson.client.codec.StringCodec());
        Class c;
        try {
            c = Class.forName("org.redisson.client.codec.StringCodec");
            config.setCodec((Codec)c.newInstance());
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Test
    public void testClusterCommand(){
        ClusterServersConfig serversConfig = config.useClusterServers()
                .setScanInterval(5000)
                .setConnectTimeout(100000)
                .setTimeout(100000);


        serversConfig.addNodeAddress("redis://10.1.236.179:6379,redis://10.1.236.179:6380,redis://10.1.236.179:6381".split(","));

        RedisConnectionFactory redisConnectionFactory = new RedissonConnectionFactory(config);
        RedisConnection redissonClusterConnection = redisConnectionFactory.getConnection();

        Map<String, String> result = redissonClusterConnection.hashCommands().hGetAll("baz");

        List<String> keys = redissonClusterConnection.keyCommands().keys("baz*");

        List<Map<String, String>> maps = redissonClusterConnection.hashCommands().hGetAll(keys);

        System.out.println(maps);

        redissonClusterConnection.close();
        //System.out.println(result);
    }

    @Test
    public void testSingleServerCommand(){
        SingleServerConfig serversConfig = config.useSingleServer();


        serversConfig.setAddress("redis://10.1.236.179:6379");

        RedisConnectionFactory redisConnectionFactory = new RedissonConnectionFactory(config);

        Map<String, String> result = redisConnectionFactory.getConnection().hashCommands().hGetAll("baz");

        redisConnectionFactory.getConnection().close();
        System.out.println(result);
    }

}