package com.woople.calcite.adapter.redis;

import java.util.HashMap;
import java.util.Map;

public class RedisTableOptions {
    private String prefixKey;
    private String[] fields;
    private String[] fieldTypes;
    private String[] keyFields;

    private Map<String, String> params = new HashMap<>();

    public String getPrefixKey() {
        return prefixKey;
    }

    public void setPrefixKey(String prefixKey) {
        this.prefixKey = prefixKey;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public String[] getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(String[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public String[] getKeyFields() {
        return keyFields;
    }

    public void setKeyFields(String[] keyFields) {
        this.keyFields = keyFields;
    }

    public String getRedisNodes() {
        return redisNodes;
    }

    public void setRedisNodes(String redisNodes) {
        this.redisNodes = redisNodes;
    }

    public RedisRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(RedisRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }

    private String redisNodes;
    private RedisRowConverter rowConverter;
}
