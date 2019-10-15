package com.woople.calcite.adapter.redis;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class RedisSchema extends AbstractSchema {
    private RedisTableOptions redisTableOptions;
    private Map<String, Table> tableMap;

    public RedisSchema(RedisTableOptions redisTableOptions) {
        this.redisTableOptions = redisTableOptions;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
            builder.put(redisTableOptions.getPrefixKey(), new RedisTable(redisTableOptions));
            tableMap = builder.build();
        }

        return tableMap;
    }
}
