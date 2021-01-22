package com.ecust.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

/**
 * @author JueQian
 * @create 01-22 10:26
 */
public class Flink03_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        //0x1 读取kafka数据创建表
        Kafka kafka = new Kafka()
                .version("0.11")
                .topic("test")
//                .property("zookeeper.connect", "hadoop102:2181")
                .property("bootstrap.servers", "hadoop102:9092")
                .property("group.id", "testGroup")
                // optional: select a startup mode for Kafka offsets
                .startFromLatest();

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.STRING())
                .field("temp", DataTypes.STRING());


        tableEnv.connect(kafka).withFormat(new Csv()).withSchema(schema).createTemporaryTable("sensor");
        /*tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("test")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "BigData0720"))
                .withFormat(new Csv())
//                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");*/

        //0x2 执行SQL查询 todo flink sql
        Table table = tableEnv.sqlQuery("select id,ts,temp from sensor where id = 'sensor_1'");

        //0x3 执行TableAPI查询 todo flink TableApi
        Table sensor = tableEnv.from("sensor");
        Table table1 = sensor.select("id,ts,temp");

        //0x3 将表数据写入到MySQL数据库 todo 创建连接
        String sinkDDL =
                "create table sensorOut (id varchar(20) not null,ts varchar(20) not null,temp varchar(20) not null) WITH (" +
                "'connector.type' = 'jdbc', " +
                "'connector.url' = 'jdbc:mysql://hadoop102:3306/test', " +
                "'connector.table' = 'sensor_id', " +
                "'connector.driver' = 'com.mysql.jdbc.Driver', " +
                "'connector.username' = 'root', " +
                "'connector.write.flush.max-rows'='1'"+
                "'connector.password' = '123456')";

        //0x3 将表转换为追加流并打印 把外部系统当做一张表,然后使用insertInto插入
        tableEnv.sqlUpdate(sinkDDL);

        table1.insertInto("sensorOut");

        //0x4 执行任务
        env.execute();
    }
}
