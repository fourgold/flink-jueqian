package com.ecust.day07;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * 使用文件连接器加处理时间
 */
public class FlinkSQL04_ProcessTime_DataSource {

    public static void main(String[] args) {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.构建文件的连接器
        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                        .field("pt", DataTypes.TIMESTAMP(3)).proctime())
                .createTemporaryTable("sensorTable");

        //3.读取数据创建表
        Table sensorTable = tableEnv.from("sensorTable");

        //4.打印表的信息
        sensorTable.printSchema();

    }

}
