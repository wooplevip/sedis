package com.woople.calcite.adapter.redis;

import org.apache.calcite.rel.type.RelDataType;

import java.util.Map;

public interface RedisRowConverter<K, V> {
    RelDataType rowDataType(String[] fields, String[] fieldTypes);
    Object[] toRow(Map<K, V> message, String[] fields);
}
