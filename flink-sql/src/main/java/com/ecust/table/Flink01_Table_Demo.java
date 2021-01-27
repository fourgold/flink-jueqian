package com.ecust.table;

import com.ecust.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author JueQian
 * @create 01-21 13:58
 */
public class Flink01_Table_Demo {
    public static void main(String[] args) throws Exception {
        //0x0 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //0x1 读取文本创建数据流并且转换为javabean,变成一个javabean流
        SingleOutputStreamOperator<SensorReading> sensorStream = env.readTextFile("data/sensor.txt").map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        //0x2 从流中创建表 能够自己从样例类中进行识别
        Table table = tableEnv.fromDataStream(sensorStream);

        //0x3 查询数据 todo table API
        Table resultOfTA = table.select("id,ts,temp").filter("id='sensor_1'");

        //0x4 转换为流输出
        tableEnv.toAppendStream(resultOfTA, Row.class).print();

        //0x5 启动任务
        env.execute();
    }
}
