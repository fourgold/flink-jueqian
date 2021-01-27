package com.ecust.time;

import com.ecust.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author JueQian
 * @create 01-22 16:57
 */
public class Flink02_EventTime_DataStream {
    public static void main(String[] args) {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //0x1 构建文件的连接器,并添加文件的事件处理时间
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> sensor = source.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTs();
            }
        });

        //0x3 将流数据转换为表并制定助理时间字段 todo 额外指定时间字段
        Table table = tableEnv.fromDataStream(sensor,"id,ts,temp,et.rowtime");
        //0x4 打印表的信息
        table.printSchema();

        //0x5 执行

    }
}
