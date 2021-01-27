package com.ecust.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

/**
 * @author JueQian
 * @create 01-21 16:05
 * 从kafka拿取数据输出到es
 */
public class Flink02_Sink_ES {
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
                .field("ts", DataTypes.BIGINT())
                .field("temp", DataTypes.DOUBLE());


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
        Table table1 = sensor.select("id,ts,temp").filter("id='sensor_1'");

        String explaination = tableEnv.explain(table);
        System.out.println(explaination);


        //0x3 将表数据写入到ES数据库 todo 创建连接
        Elasticsearch es = new Elasticsearch()
                .version("6")
                .host("hadoop102", 9200, "http")
                .index("sensor1")
                .documentType("_doc")
                .bulkFlushMaxActions(1);
        tableEnv.connect(es)
                .withFormat(new Json())
                .withSchema(schema)
                .inAppendMode()//指定模式>>>>>>>>>>>>
                .createTemporaryTable("sensorES");

        //0x3 将表转换为追加流并打印 把外部系统当做一张表,然后使用insertInto插入
        tableEnv.insertInto("sensorES",table1);

        //0x4 执行任务
        env.execute();

    }
}
