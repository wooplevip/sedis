package com.woople.calcite.adapter.redis;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.Map;

@Ignore
public class RedisTableTest {
    Statement statement = null;
    @Before
    public void init() throws Exception{
        String model = "{\"version\":\"1.0\",\"defaultSchema\":\"SEDIS\",\"schemas\":[{\"name\":\"SEDIS\",\"type\":\"custom\",\"factory\":\"com.woople.calcite.adapter.redis.RedisSchemaFactory\",\"operand\":{\"sedis.redis.cluster.nodes\":\"10.1.236.179:6379,10.1.236.179:6380,10.1.236.179:6381\",\"sedis.redis.table\":{\"tableName\":\"BAZ\",\"fields\":\"ID:VARCHAR,NAME:VARCHAR\",\"keys\":\"ID\"}}}]}";
        Connection connection = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
        statement = connection.createStatement();

    }

    @Test
    public void testRedisTable() throws Exception{
        ResultSet resultSet = statement.executeQuery("select * from BAZ where ID='2'");
        System.out.println(getData(resultSet));

        System.out.println("===");
    }

    private List<Map<String,Object>> getData(ResultSet resultSet)throws Exception{
        List<Map<String,Object>> list = Lists.newArrayList();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();

        while (resultSet.next()) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                map.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
            list.add(map);
        }
        return list;
    }

}