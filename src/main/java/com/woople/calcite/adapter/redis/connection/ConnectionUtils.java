package com.woople.calcite.adapter.redis.connection;

import com.woople.calcite.adapter.redis.connection.redisson.RedissonConnectionFactory;

public abstract class ConnectionUtils {

	public static boolean isRedisson(RedisConnectionFactory connectionFactory) {
		return connectionFactory instanceof RedissonConnectionFactory;
	}

	public static boolean isJedis(RedisConnectionFactory connectionFactory) {
		return connectionFactory instanceof JedisConnectionFactory;
	}
}
