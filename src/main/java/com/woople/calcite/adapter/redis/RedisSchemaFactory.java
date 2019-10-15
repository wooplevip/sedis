package com.woople.calcite.adapter.redis;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class RedisSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        final RedisTableOptions tableOptionBuilder = new RedisTableOptions();
        tableOptionBuilder.setRedisNodes(operand.get(RedisTableConstants.SEDIS_REDIS_CLUSTER_NODES).toString());
        Map<String, String> tableInfo = (LinkedHashMap)operand.get(RedisTableConstants.SEDIS_REDIS_TABLE);

        String[] fieldsInfo = tableInfo.get("fields").split(",");
        String[] fields = new String[fieldsInfo.length];
        String[] fieldTypes = new String[fieldsInfo.length];

        for (int i = 0; i < fieldsInfo.length; i++) {
            String[] field = fieldsInfo[i].split(":");
            fields[i] = field[0];
            fieldTypes[i] = field[1];
        }

        tableOptionBuilder.setFields(fields);
        tableOptionBuilder.setFieldTypes(fieldTypes);
        tableOptionBuilder.setKeyFields(tableInfo.get("keys").split(","));
        tableOptionBuilder.setPrefixKey(tableInfo.get("tableName"));

        final RedisRowConverter rowConverter;
        if (operand.containsKey(RedisTableConstants.SCHEMA_ROW_CONVERTER)) {
            String rowConverterClass = (String) operand.get(RedisTableConstants.SCHEMA_ROW_CONVERTER);
            try {
                final Class<?> rlass = Class.forName(rowConverterClass);
                rowConverter = (RedisRowConverter) rlass.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | InvocationTargetException
                    | IllegalAccessException | ClassNotFoundException
                    | NoSuchMethodException e) {
                final String details = String.format(Locale.ROOT,
                        "Failed to create table '%s' with configuration:\n"
                                + "'%s'\n"
                                + "RedisRowConverter '%s' is invalid",
                        name, operand, rowConverterClass);
                throw new RuntimeException(details, e);
            }
        } else {
            rowConverter = new RedisRowConverterImpl();
        }
        tableOptionBuilder.setRowConverter(rowConverter);

        return new RedisSchema(tableOptionBuilder);
    }
}
