package com.woople.calcite.adapter.redis.connection;

import com.woople.calcite.adapter.redis.util.ObjectUtils;

public class RedisNode {

	String id;
	String name;
	String host;
	int port;
	NodeType type;
	String masterId;

	public RedisNode(String host, int port) {
		this.host = host;
		this.port = port;
	}

	protected RedisNode() {}

	public String getHost() {
		return host;
	}

	public Integer getPort() {
		return port;
	}

	public String asString() {
		return host + ":" + port;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMasterId() {
		return masterId;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public NodeType getType() {
		return type;
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public boolean isMaster() {
		return ObjectUtils.nullSafeEquals(NodeType.MASTER, getType());
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public boolean isSlave() {
		return isReplica();
	}

	/**
	 * @return
	 * @since 2.1
	 */
	public boolean isReplica() {
		return ObjectUtils.nullSafeEquals(NodeType.SLAVE, getType());
	}

	/**
	 * Get {@link RedisNodeBuilder} for creating new {@link RedisNode}.
	 *
	 * @return never {@literal null}.
	 * @since 1.7
	 */
	public static RedisNodeBuilder newRedisNode() {
		return new RedisNodeBuilder();
	}

	@Override
	public String toString() {
		return asString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ObjectUtils.nullSafeHashCode(host);
		result = prime * result + ObjectUtils.nullSafeHashCode(port);
		return result;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}
		if (obj == null || !(obj instanceof RedisNode)) {
			return false;
		}

		RedisNode other = (RedisNode) obj;

		if (!ObjectUtils.nullSafeEquals(this.host, other.host)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(this.port, other.port)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(this.name, other.name)) {
			return false;
		}

		return true;
	}

	public enum NodeType {
		MASTER, SLAVE
	}

	public static class RedisNodeBuilder {

		private RedisNode node;

		public RedisNodeBuilder() {
			node = new RedisNode();
		}

		public RedisNodeBuilder withName(String name) {
			node.name = name;
			return this;
		}

		public RedisNodeBuilder listeningAt(String host, int port) {
			node.host = host;
			node.port = port;
			return this;
		}

		public RedisNodeBuilder withId(String id) {

			node.id = id;
			return this;
		}

		public RedisNodeBuilder promotedAs(NodeType type) {

			node.type = type;
			return this;
		}

		public RedisNodeBuilder slaveOf(String masterId) {
			return replicaOf(masterId);
		}

		public RedisNodeBuilder replicaOf(String masterId) {

			node.masterId = masterId;
			return this;
		}

		public RedisNode build() {
			return this.node;
		}
	}

}
