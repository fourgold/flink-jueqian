package practice;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

public class Flink04_Watermark_SlideWindow {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取端口数据创建流
        SingleOutputStreamOperator<String> socketTextStream = env.socketTextStream("hadoop102", 9999).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[0], 1);
            }
        });

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //5.开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("SideOutPut") {
                });

        //6.聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);

        //7.打印
        result.print("result");
        result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("SideOutPut") {
        }).print("SideOutPut");

        //8.执行
        env.execute();

    }

}
