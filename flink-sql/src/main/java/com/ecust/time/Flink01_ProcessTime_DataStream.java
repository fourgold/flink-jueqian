package com.ecust.time;

import com.ecust.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author JueQian
 * @create 01-22 16:57
 */
public class Flink01_ProcessTime_DataStream {
    public static void main(String[] args) {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //0x1 读取端口数据然后将数据转换为SensorReading
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> sensor = source.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        //0x3 将流数据转换为表并制定助理时间字段 todo 使用时间处理时间
        Table table = tableEnv.fromDataStream(sensor, "id,ts,temp,pc.proctime");

        //0x4 打印表的信息
        table.printSchema();

        //0x5 执行

    }
}
