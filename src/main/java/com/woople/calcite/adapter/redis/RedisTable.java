package com.woople.calcite.adapter.redis;

import com.woople.calcite.adapter.redis.util.RedissonConfigHelper;
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
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.Codec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisTable extends AbstractTable implements FilterableTable {
    protected RelProtoDataType protoRowType;

    private RedisTableOptions redisTableOptions;

    public RedisTable(RedisTableOptions redisTableOptions) {
        this.redisTableOptions = redisTableOptions;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
//        Config config = new Config();
//        config.setCodec(new org.redisson.client.codec.StringCodec());
//        Class c;
//        try {
//            c = Class.forName("org.redisson.client.codec.StringCodec");
//            config.setCodec((Codec)c.newInstance());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        ClusterServersConfig serversConfig = config.useClusterServers()
//                .setScanInterval(5000)
//                .setConnectTimeout(100000)
//                .setTimeout(100000);
//
//        for (String node : redisTableOptions.getRedisNodes().split(",")) {
//            serversConfig.addNodeAddress("redis://" + node);
//        }
        Config config = null;
        try {
            Map<String,String> redisProps ;
            if ( redisTableOptions.getParams() == null ){
                redisProps = new HashMap<>();
            }else {
                redisProps = redisTableOptions.getParams();
            }
            redisProps.put(RedisTableConstants.SEDIS_REDIS_CLUSTER_NODES,redisTableOptions.getRedisNodes());
            config = RedissonConfigHelper.createConfig(redisProps);
        } catch (Exception e) {
            e.printStackTrace();
        }

        RedissonClient redissonClient = Redisson.create(config);

        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new RedisMessageEnumerator(redissonClient, redisTableOptions, cancelFlag);
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
