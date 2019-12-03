package com.woople.calcite.adapter.redis.util;

import com.woople.calcite.adapter.redis.RedisTableConstants;
import org.apache.commons.lang3.StringUtils;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;

import java.util.Arrays;
import java.util.Map;

public class ClusterServerConfigHelper {
    public static ClusterServersConfig createClusterServerConfig(Map<String, String> props, Config config) {
        ClusterServersConfig csc = config.useClusterServers();
        csc.addNodeAddress(getNodeList(props.get(RedisTableConstants.SEDIS_REDIS_CLUSTER_NODES)));
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SCAN_INTERVAL))) {
            csc.setScanInterval(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_SCAN_INTERVAL)));
        }
        BaseMasterSlaveServersConfigHelper.resloveConfig(props, csc);
        return csc;
    }

    private static String[] getNodeList(String servers) {
        String[] serverArray = StringUtils.split(servers, ",");
        return Arrays.stream(serverArray).map(ClusterServerConfigHelper::toRedisAddress).toArray(x -> new String[serverArray.length]);
    }

    private static String toRedisAddress(String server) {
        return Constant.REDIS_URL_PERFIX + server;
    }
}
