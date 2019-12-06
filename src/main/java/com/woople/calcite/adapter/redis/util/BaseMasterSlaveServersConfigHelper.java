package com.woople.calcite.adapter.redis.util;

import com.woople.calcite.adapter.redis.util.Constant;
import org.apache.commons.lang3.StringUtils;
import org.redisson.config.BaseMasterSlaveServersConfig;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/***
 * This class helps resloving configs in BaseMasterSlaveServersConfig.java and BaseConfig.java
 */
public class BaseMasterSlaveServersConfigHelper {
    public static void resloveConfig(Map<String, String> props, BaseMasterSlaveServersConfig bmsc) {

        // Resolve BaseMasterSlaveServersConfig
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFG_SLAVE_CONNECTION_MINiMUM_IDLE_SIZE))) {
            bmsc.setSlaveConnectionMinimumIdleSize(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFG_SLAVE_CONNECTION_MINiMUM_IDLE_SIZE)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SLAVE_CONNECTION_POOL_SIZE))) {
            bmsc.setSlaveConnectionPoolSize(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_SLAVE_CONNECTION_POOL_SIZE)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_FAILED_SLAVE_CONNECTION_INTERVAL))) {
            bmsc.setFailedSlaveCheckInterval(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_FAILED_SLAVE_CONNECTION_INTERVAL)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_FAILED_SLAVE_CHECK_INTERVAL))) {
            bmsc.setFailedSlaveCheckInterval(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_FAILED_SLAVE_CHECK_INTERVAL)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_MASTER_CONNECTION_MINIMUM_IDLE_SIZE))) {
            bmsc.setMasterConnectionMinimumIdleSize(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_MASTER_CONNECTION_MINIMUM_IDLE_SIZE)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_MASTER_CONNECTION_POOL_SIZE))) {
            bmsc.setMasterConnectionPoolSize(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_MASTER_CONNECTION_POOL_SIZE)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SUBSCRIPTION_CONNECTION_MINIMUM_IDLE_SIZE))) {
            bmsc.setSubscriptionConnectionMinimumIdleSize(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_SUBSCRIPTION_CONNECTION_MINIMUM_IDLE_SIZE)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SUBSCRIPTION_CONNECTION_POOL_SIZE))) {
            bmsc.setSubscriptionConnectionPoolSize(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_SUBSCRIPTION_CONNECTION_POOL_SIZE)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_DNS_MONITORING_INTERVAL))) {
            bmsc.setDnsMonitoringInterval(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_DNS_MONITORING_INTERVAL)));
        }


        // Resolve the BaseConfig

        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_IDLE_CONNECTION_TIMEOUT))) {
            bmsc.setSlaveConnectionMinimumIdleSize(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_DNS_MONITORING_INTERVAL)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_PING_TIMEOUT))) {
            bmsc.setPingTimeout(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_PING_TIMEOUT)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_CONNECT_TIMEOUT))) {
            bmsc.setConnectTimeout(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_CONNECT_TIMEOUT)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_TIMEOUT))) {
            bmsc.setTimeout(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_TIMEOUT)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_RETRY_ATTEMPTS))) {
            bmsc.setRetryAttempts(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_RETRY_ATTEMPTS)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_RETRY_INTERVAL))) {
            bmsc.setRetryInterval(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_RETRY_INTERVAL)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_CACHE_SERVER_PASSWORD))) {
            bmsc.setPassword(props.get(Constant.SIDES_CACHE_SERVER_PASSWORD));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SUBSCRIPTION_PER_CONNECTION))) {
            bmsc.setSubscriptionsPerConnection(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_SUBSCRIPTION_PER_CONNECTION)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_CLIENT_NAME))) {
            bmsc.setClientName(props.get(Constant.SIDES_REDIS_CONFIG_CLIENT_NAME));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SSL_ENABLE_ENDPOINT_IDENTIFICATION))) {
            bmsc.setSslEnableEndpointIdentification(Boolean.parseBoolean(props.get(Constant.SIDES_REDIS_CONFIG_SSL_ENABLE_ENDPOINT_IDENTIFICATION)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SSL_TRUSTSTORE))) {
//            try {
//                bmsc.setSslTruststore(new URI(props.get(Constant.SIDES_REDIS_CONFIG_SSL_TRUSTSTORE)));
//            } catch (URISyntaxException e) {
//                e.printStackTrace();
//            }
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SSL_TRUSTSTORE_PASSWORD))) {
            bmsc.setSslTruststorePassword(props.get(Constant.SIDES_REDIS_CONFIG_SSL_TRUSTSTORE_PASSWORD));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SSL_KEYSTORE))) {
//            try {
////                bmsc.setSslKeystore(new URI(props.get(Constant.SIDES_REDIS_CONFIG_SSL_KEYSTORE)));
//            } catch (URISyntaxException e) {
//                e.printStackTrace();
//            }
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_SSL_KEYSTORE_PASSWORD))) {
            bmsc.setSslKeystorePassword(props.get(Constant.SIDES_REDIS_CONFIG_SSL_KEYSTORE_PASSWORD));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_PING_CONNECTION_INTERVAL))) {
            bmsc.setPingConnectionInterval(Integer.valueOf(props.get(Constant.SIDES_REDIS_CONFIG_PING_CONNECTION_INTERVAL)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_KEEP_ALIVE))) {
            bmsc.setKeepAlive(Boolean.parseBoolean(props.get(Constant.SIDES_REDIS_CONFIG_KEEP_ALIVE)));
        }
        if (StringUtils.isNotEmpty(props.get(Constant.SIDES_REDIS_CONFIG_TCP_NO_DELAY))) {
            bmsc.setTcpNoDelay(Boolean.parseBoolean(props.get(Constant.SIDES_REDIS_CONFIG_TCP_NO_DELAY)));
        }

    }
}
