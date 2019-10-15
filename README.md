
# sedis

***S***QL for R***edis***

> 基于[Apache Calcite](http://calcite.apache.org/)实现了通过jdbc的方法连接[Redis  Cluster](https://redis.io/topics/cluster-tutorial)



## 快速入门


1. .进入项目根目录，使用maven打包

   `mvn clean package`打包完成后，会在target的目录里面生成一个sedis-0.1-bundle.jar包

2. 在Redis cluster中创建HASH类型的数据，例如创建两个key

   ```shell
   >hmset BAZ:1 ID 1 NAME spark
   >hmset BAZ:2 ID 2 NAME flink
   ```
3. 通过jdbc客户端写sql查询
  
   在pom中配置
    ```xml
    <dependency>
        <groupId>com.woople</groupId>
        <artifactId>sedis</artifactId>
        <version>0.1-bundle</version>
        <scope>system</scope>
        <systemPath>/tmp/sedis-0.1-bundle.jar</systemPath>
    </dependency>
    ```
    客户端代码
    ```java
    import java.sql.*;
    import java.util.LinkedHashMap;
    import java.util.Map;

    public class SedisExamples {
        public static void main(String[] args) throws Exception{
            String model = "{\"version\":\"1.0\",\"defaultSchema\":\"SEDIS\",\"schemas\":[{\"name\":\"SEDIS\",\"type\":\"custom\",\"factory\":\"com.woople.calcite.adapter.redis.RedisSchemaFactory\",\"operand\":{\"sedis.redis.cluster.nodes\":\"10.1.236.179:6379,10.1.236.179:6380,10.1.236.179:6381\",\"sedis.redis.table\":{\"tableName\":\"BAZ\",\"fields\":\"ID:VARCHAR,NAME:VARCHAR\",\"keys\":\"ID\"}}}]}";
            Connection connection = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from BAZ where ID='2'");
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnSize = metaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, Object> result = new LinkedHashMap<>();
                for (int i = 1; i < columnSize + 1; i++) {
                    result.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                }
                System.out.println(result);
            }

            statement.close();
            connection.close();
        }
    }
    ```
    输出结果
   
    `{ID=2, NAME=flink}`
    
## 特性

目前0.1版本为简单的体验版

1. 目前只支持Redis cluster中的HASH结构的数据
2. 只支持`select`查询语句

## 后续计划

1. 支持Redis单机模式
2. 优化参数配置
3. 支持`update`和`insert`语句