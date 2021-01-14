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
 * 在指定时间语义的时候,测试一个问题
 * 就是我们输入一个数据是一个string
 * 然后指定其中的时间语义字段
 * 然后将其转变为Tuple3
 * 使用Tuple3进行分组使用求温度最大值,
 *      todo 测试其时间语义还能否有效
 *      测试证明也是可行的
 *
 *     结论:指定时间语义是从字段中抽取出来的字段当做 waterMark()
 */
public class Flink03_Window_WaterMark_TimeTest {
    public static void main(String[] args) throws Exception {

        //0x0 定义执行环境,并且从端口读取数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketSource = env.socketTextStream("hadoop102", 9999);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> assignTimestampsAndWatermarks = socketSource.
                assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<String>
                                (Time.milliseconds(200)) {
            @Override
            public long extractTimestamp(String s) {
                String[] fields = s.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });

        //0x1 将数据添加上数据结构
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> mapStream = assignTimestampsAndWatermarks.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple3<>(fields[0], Long.parseLong(fields[1])*1000, Double.parseDouble(fields[2]));
            }
        });

        /*//0x2 指定时间语义 设定waterMark为两秒
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> watermarks = mapStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Double>>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(Tuple3<String, Long, Double> stringLongDoubleTuple3) {
                return stringLongDoubleTuple3.f1;
            }
        });*/

        //0x3 开窗,设定窗口时间与测输出流
        WindowedStream<Tuple3<String, Long, Double>, Tuple, TimeWindow> timeWindow = mapStream.keyBy(1).timeWindow(Time.seconds(5));
        WindowedStream<Tuple3<String, Long, Double>, Tuple, TimeWindow> windowedStream = timeWindow
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple3<String, Long, Double>>("side") {});

        SingleOutputStreamOperator<Tuple3<String, Long, Double>> streamOperator = windowedStream.maxBy(2);
        //0x4 打印
        streamOperator.print("主输出流");
        streamOperator.getSideOutput(new OutputTag<Tuple3<String, Long, Double>>("side") {}).print("测输出流");

        //0x5 执行
        env.execute();
    }
}
