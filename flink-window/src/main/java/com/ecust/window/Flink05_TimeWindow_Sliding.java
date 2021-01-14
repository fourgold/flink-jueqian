package com.ecust.window;

import com.ecust.beans.SensorReading;
import com.ecust.source.Flink05_Source_UDFSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author JueQian
 * @create 01-12 16:38
 * 全量窗口函数
 */
public class Flink05_TimeWindow_Sliding {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
            //todo 定义时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //0x2 读取自定义流
        DataStreamSource<SensorReading> sensorSource = env.addSource(new Flink05_Source_UDFSource.MySource());

            //todo 指定事件时间
        SingleOutputStreamOperator<SensorReading> sensorWithTime = sensorSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
            @Override
            public long extractAscendingTimestamp(SensorReading sensorReading) {
                return sensorReading.getTs();
            }
        });

        //0x3 分组
        KeyedStream<SensorReading, String> one2Sensor = sensorWithTime.keyBy(SensorReading::getId);

        //0x4 开窗 todo 加一个参数变成滑动时间窗口
        SingleOutputStreamOperator<SensorReading> result = one2Sensor.timeWindow(Time.seconds(15),Time.seconds(3)).min("temp");


        //0x5 打印数据
        result.print();

        //0x6 执行
        env.execute();
    }
}
