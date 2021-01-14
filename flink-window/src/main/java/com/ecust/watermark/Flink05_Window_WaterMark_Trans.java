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
 * 测试WaterMark在流中的传递问题
 * 生成条件:在executor内部指定时间时间语义
 * 规则:  1.water在shuffle过程中向下游算子广播
 *        2.接收到所有此阶段的窗口的数据后取其中最小的waterMark
 */
public class Flink05_Window_WaterMark_Trans {
    public static void main(String[] args) throws Exception {

        //0x0 定义执行环境,并且从端口读取数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> socketSource = env.socketTextStream("hadoop102", 9999);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //0x1 将数据添加上数据结构
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> mapStream = socketSource.map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple3<>(fields[0], Long.parseLong(fields[1])*1000, Double.parseDouble(fields[2]));
            }
        });

        //0x2 在数据结构之后为数据指定时间戳,这样就会造成了在不同的executor生成了不同的waterMark
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> timestamps = mapStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Double>>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(Tuple3<String, Long, Double> stringLongDoubleTuple3) {
                return stringLongDoubleTuple3.f1 * 1000L;
            }
        });

        //0x3 开窗,设定窗口时间与测输出流
        WindowedStream<Tuple3<String, Long, Double>, Tuple, TimeWindow> timeWindow = timestamps.keyBy(1).timeWindow(Time.seconds(5));
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
