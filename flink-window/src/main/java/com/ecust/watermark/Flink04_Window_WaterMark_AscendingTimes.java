package com.ecust.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * @author JueQian
 * @create 01-13 11:43
 * 测试一下在文件流经常使用的自增时间
 */
public class Flink04_Window_WaterMark_AscendingTimes {
    public static void main(String[] args) throws Exception {

        //0x0 定义执行环境,并且从端口读取数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketSource = env.socketTextStream("hadoop102", 9999);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //使用自增时间语义
        SingleOutputStreamOperator<String> assignTimestampsAndWatermarks = socketSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String s) {
                String[] fields = s.split(",");
                return Long.parseLong(fields[1])*1000L;
            }
        });



        //0x1 将数据添加上数据结构 进行第一个map的时候是进行hash分布
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> mapStream = assignTimestampsAndWatermarks.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple3<>(fields[0], Long.parseLong(fields[1])*1000, Double.parseDouble(fields[2]));
            }
        });

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
