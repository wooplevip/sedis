package com.woople.calcite.adapter.redis;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

public class RedisRowConverterImpl implements RedisRowConverter<String, String> {

    @Override
    public RelDataType rowDataType(String[] fields, String[] fieldTypes) {

        final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        for (int i = 0; i < fields.length; i++) {
            fieldInfo.add(fields[i], typeFactory.createSqlType(SqlTypeName.get(fieldTypes[i])));
        }
        return fieldInfo.build();
    }

    @Override
    public Object[] toRow(Map<String, String> message, String[] fields) {
        Object[] fieldValues = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldValues[i] = message.get(fields[i]);
        }
        return fieldValues;
    }
}
