package com.woople.calcite.adapter.redis;

import com.woople.calcite.adapter.redis.connection.RedisConnection;
import com.woople.calcite.adapter.redis.connection.RedisConnectionFactory;
import com.woople.calcite.adapter.redis.connection.redisson.RedissonConnectionFactory;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.redisson.client.codec.Codec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisTable extends AbstractTable implements FilterableTable {
    private final Logger logger = LoggerFactory.getLogger(AbstractTable.class);
    protected RelProtoDataType protoRowType;

    private RedisTableOptions redisTableOptions;

    public RedisTable(RedisTableOptions redisTableOptions) {
        this.redisTableOptions = redisTableOptions;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        Config config = new Config();
        config.setCodec(new org.redisson.client.codec.StringCodec());
        Class c;
        try {
            c = Class.forName("org.redisson.client.codec.StringCodec");
            config.setCodec((Codec)c.newInstance());
        } catch (Exception e) {
            e.printStackTrace();
        }

        String connectionType = this.redisTableOptions.getParams().getOrDefault(RedisTableConstants.SEDIS_REDIS_CONNECTION, "redisson");
        RedisConnectionFactory redisConnectionFactory = null;
        if (connectionType.equals("redisson")){
            String[] nodes = redisTableOptions.getRedisNodes().split(",");
            if (nodes.length > 1){
                ClusterServersConfig serversConfig = config.useClusterServers()
                        .setScanInterval(5000)
                        .setConnectTimeout(100000)
                        .setTimeout(100000);
                for (String node : nodes) {
                    serversConfig.addNodeAddress("redis://" + node);
                }
            }else {
                SingleServerConfig serversConfig = config.useSingleServer()
                        .setConnectTimeout(100000)
                        .setTimeout(100000);

                serversConfig.setAddress("redis://" + nodes[0]);
            }

            redisConnectionFactory = new RedissonConnectionFactory(config);
        }

        RedisConnection redisConnection = redisConnectionFactory.getConnection();

        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new RedisMessageEnumerator(redisConnection, redisTableOptions, cancelFlag);
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }

        return redisTableOptions.getRowConverter().rowDataType(redisTableOptions.getFields(), redisTableOptions.getFieldTypes());
    }
}
