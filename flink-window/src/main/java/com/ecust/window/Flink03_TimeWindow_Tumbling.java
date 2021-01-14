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
 * 需求:统计自定义数据源的传感器数据
 * 开窗5秒的最小温度
 *
 * 使用:
 * 事件时间语义
 * timeWindow
 * 滚动窗口
 *
 * 关于格林兰治的时间
 */
public class Flink03_TimeWindow_Tumbling {
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

        //0x4 开窗
        SingleOutputStreamOperator<SensorReading> result = one2Sensor.timeWindow(Time.seconds(5)).min("temp");

        //由于是UTC格林兰治的时间
//        one2Sensor.timeWindow(Time.days(1));//这个window是早八点到八点
        //如果国内想开一天的窗口
//        one2Sensor.window(TumblingProcessingTimeWindows.of(Time.days(1),Time.hours(-8)));

        //0x5 打印数据
        result.print();

        //0x6 执行
        env.execute();
    }
}
