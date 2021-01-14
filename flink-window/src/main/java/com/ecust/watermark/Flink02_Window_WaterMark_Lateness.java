package com.ecust.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * @author JueQian
 * @create 01-13 11:43
 * 延迟发车 waterMark 包容数据的混乱度
 * 允许迟到时间发车等人 allowedLateness 允许迟到数据,包容网络的延迟
 * 侧输出流 sideOutputStream 保证数据的准确一致性(牺牲了一点准确性,保证了高效性)
 * todo 测输出流不参与计算,需要统计然后单独处理
 */
public class Flink02_Window_WaterMark_Lateness {
    public static void main(String[] args) throws Exception {

        //0x0 定义执行环境,并且从端口读取数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketSource = env.socketTextStream("hadoop102", 9999);

        //0x1 将数据添加上数据结构
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> mapStream = socketSource.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple3<>(fields[0], Long.parseLong(fields[1])*1000, Double.parseDouble(fields[2]));
            }
        });

        //0x2 指定时间语义 设定waterMark为两秒
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> watermarks = mapStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Double>>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(Tuple3<String, Long, Double> stringLongDoubleTuple3) {
                return stringLongDoubleTuple3.f1;
            }
        });

        //0x3 开窗,设定窗口时间与测输出流
        WindowedStream<Tuple3<String, Long, Double>, Tuple, TimeWindow> timeWindow = watermarks.keyBy(1).timeWindow(Time.seconds(5));
        WindowedStream<Tuple3<String, Long, Double>, Tuple, TimeWindow> windowedStream = timeWindow
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple3<String, Long, Double>>("side") {});
                //计算窗口时间内的温度的最大值
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> streamOperator = windowedStream.maxBy(2);
        //0x4 打印
        streamOperator.print("主输出流");
        streamOperator.getSideOutput(new OutputTag<Tuple3<String, Long, Double>>("side") {}).print("测输出流");

        //0x5 执行
        env.execute();
    }
}
