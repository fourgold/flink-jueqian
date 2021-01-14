package com.ecust.window;

import com.ecust.beans.SensorReading;
import com.ecust.source.Flink05_Source_UDFSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author JueQian
 * @create 01-12 11:59
 * 概述:
 * 窗口类型分两种:时间窗口 计数窗口
 * 窗口功能分三种:滚动 滑动 会话(时间窗口特有)
 * 窗口方法分两种:增量(aggregate:sum,min,max)全量(apply)
 */
public class Flink01_TimeWindow {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //0x1 从执行环境中获取
        DataStreamSource<SensorReading> sensorSource = env.addSource(new Flink05_Source_UDFSource.MySource());

        //0x2 使用时间窗口统计每个窗口的最高温度
        WindowedStream<SensorReading, String, TimeWindow> windowStream = sensorSource.keyBy(SensorReading::getId).timeWindow(Time.seconds(5));

        SingleOutputStreamOperator<SensorReading> temp = windowStream.maxBy("temp");

        //0x3 打印
        temp.print("最高温度");

        //0x4 执行
        env.execute();
    }
}
