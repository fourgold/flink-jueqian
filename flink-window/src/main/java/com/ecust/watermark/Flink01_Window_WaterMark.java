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

/**
 * @author JueQian
 * @create 01-13 9:20
 * 使用watermark
 * 周期性:200ms
 * 断点式:
 * waterMark 窗口内左闭右开
 * waterMark窗口关闭时间
 */
public class Flink01_Window_WaterMark {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境并获取自定义数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sensor = env.socketTextStream("hadoop102", 9999);

        //0x1 指定使用时间事件语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //todo 下面转换结构之后 制定了时间语义不起作用
        /*//0x2 指定时间语义字段 指定知道数据为2,是自增长允许的最大乱序程度,指定waterMark
        SingleOutputStreamOperator<String> timeStream = sensor.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String s) {
                return Long.parseLong(s.split(",")[1]);
            }
        });*/

        //0x3 map转换成tuple
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> map = sensor.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple3<>(fields[0], Long.parseLong(fields[1])*1000L, Double.parseDouble(fields[2]));
            }
        });


        //0x4 在这里真正的指定时间语义 指定waterMark=2s
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> tuple3SingleOutputStreamOperator = map
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Double>>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(Tuple3<String, Long, Double> stringLongDoubleTuple3) {
                return stringLongDoubleTuple3.f1;
            }
        });

        //0x5 分组开窗
        WindowedStream<Tuple3<String, Long, Double>, Tuple, TimeWindow> windowedStream = tuple3SingleOutputStreamOperator.keyBy(0).timeWindow(Time.seconds(5));


        //0x6 增量聚合求最大温度
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> max = windowedStream.maxBy(2);

        //0x7 打印数据
        max.print();


        //0x8 使用
        env.execute();
    }
}
