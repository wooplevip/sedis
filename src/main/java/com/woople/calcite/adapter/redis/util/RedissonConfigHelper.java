package com.woople.calcite.adapter.redis.util;

import com.woople.calcite.adapter.redis.RedisTableConstants;
import org.apache.commons.lang3.StringUtils;
import org.redisson.client.codec.Codec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RedissonConfigHelper {

//    private static String REDIS_URL_PERFIX = "redis://";

    private static Logger logger = LoggerFactory.getLogger(RedissonConfigHelper.class);

    public static Config createConfig(Map<String, String> props) throws Exception {
        Config config = new Config();
        resloveConfig(props, config);
        return config;
    }

    /***
     * This method helps resloving config with custom properties.
     *
     * Note : eventLoopGroup,transportMode,executor are not supported to custom.
     * @param props : custom properties
     * @param config
     */
    private static void resloveConfig(Map<String, String> props, Config config) throws Exception {
        // set codec
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_CODEC))) {
            try {
                config.setCodec((Codec) Class.forName(props.get(Constant.SIDES_REDIS_CONFIG_CODEC).toString()).newInstance());
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                logger.error("Something wrong happens when initializing codec. Ignoring it. Exception : ", e);
            }
        } else {
            config.setCodec(new org.redisson.client.codec.StringCodec());
        }

        // set threads
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_THREADS))) {
            config.setThreads(Integer.parseInt(props.get(Constant.SIDES_REDIS_CONFIG_THREADS)));
        }

        // set netty threads
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_NETTY_THREADS))) {
            config.setNettyThreads(Integer.parseInt(props.get(Constant.SIDES_REDIS_CONFIG_NETTY_THREADS)));
        }

        // set referenceEnabled
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_REFERENCE_ENABLE))) {
            config.setReferenceEnabled(Boolean.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_REFERENCE_ENABLE)));
        }

        // set lockWatchdogTimeout
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_LOCKWATCHDOG_TIMEOUT))) {
            config.setLockWatchdogTimeout(Integer.parseInt(props.get(Constant.SIDES_REDIS_CONFIG_LOCKWATCHDOG_TIMEOUT)));
        }

        // set keepPubSubOrder
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_KEEPPUBSUBORDER))) {
            config.setReferenceEnabled(Boolean.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_KEEPPUBSUBORDER)));
        }

        // set decodeInExecutor
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_DECODE_IN_EXECUTOR))) {
            config.setDecodeInExecutor(Boolean.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_DECODE_IN_EXECUTOR)));
        }

        // set useScriptCache
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_USE_SCRIPT_CACHE))) {
            config.setUseScriptCache(Boolean.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_USE_SCRIPT_CACHE)));
        }

        // set minCleanUpDelay
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_MIN_CLEAN_UP_DELAY))) {
            config.setMinCleanUpDelay(Integer.parseInt(props.get(Constant.SIDES_REDIS_CONFIG_MIN_CLEAN_UP_DELAY)));
        }

        //set maxCleanUpDelay
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_MAX_CLEAN_UP_DELAY))) {
            config.setMinCleanUpDelay(Integer.parseInt(props.get(Constant.SIDES_REDIS_CONFIG_MAX_CLEAN_UP_DELAY)));
        }

        String redisDeployMode = props.getOrDefault(Constant.SIDES_REDIS_MODE, Constant.SIDES_REDIS_MODE_DEFAULT).toUpperCase();
        switch (RedisMode.valueOf(redisDeployMode.toUpperCase())) {
            case CLUSTER:
                if (StringUtils.isEmpty(props.get(RedisTableConstants.SEDIS_REDIS_CLUSTER_NODES))) {
                    throw new Exception("Redis Server can't be null");
                }
                ClusterServerConfigHelper.createClusterServerConfig(props, config);

                break;
            default:
                logger.error("Not Supported redis mode.");
                break;
        }
    }

//
//    public static void main(String[] args) throws Exception {
//
//        Config config =  RedissonConfigHelper.createConfig(new HashMap<String,String>());
//        String test = "123:234,743:437";
//        RedissonClient redisson = Redisson.create(config);
//        RBatch rBatch = redisson.createBatch();
//        Map<String, String> m = new HashMap<String,String>();
//        m.put("a", "");
//        m.put("b","B");
//
//        for (int i =0 ; i< 10000 ; i++){
//            rBatch.getMap("key" + i).putAllAsync(m);
//        }
//
//        Future<BatchResult<?>> resFuture = rBatch.executeAsync();
//
////        List<String> ret = getNodeList(test);
//        System.out.println("success");
//    }
}

