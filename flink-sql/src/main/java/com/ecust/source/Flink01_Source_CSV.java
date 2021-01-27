package com.ecust.source;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author JueQian
 * @create 01-21 15:01
 */
public class Flink01_Source_CSV {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        //0x1 读取文件数据创建表
        tableEnv.connect(new FileSystem().path("data/sensor.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");
        //0x2 执行SQL查询 todo flink sql
        Table table = tableEnv.sqlQuery("select id,ts,temp from sensor where id = 'sensor_1'");

        //0x3 执行TableAPI查询 todo flink TableApi
        Table sensor = tableEnv.from("sensor");
        Table table1 = sensor.select("id,ts,temp").filter("id='sensor_1'");

        //0x3 将表转换为追加流并打印
        tableEnv.toAppendStream(table, Row.class).print("sql");
        tableEnv.toAppendStream(table1, Row.class).print("table");

        //0x4 执行任务
        env.execute();
    }
}
