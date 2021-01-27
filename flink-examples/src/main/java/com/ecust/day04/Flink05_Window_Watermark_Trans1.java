package day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

//滚动时间窗口,计算最近10秒数据的WordCount
public class Flink05_Window_Watermark_Trans1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //引入事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取端口数据创建流fei
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //指定数据中的时间字段
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = stringSingleOutputStreamOperator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[0], 1);
            }
        });

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //5.开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));

        //6.聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);

        //7.打印
        result.print();

        //8.执行
        env.execute();

    }
}
