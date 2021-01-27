package day03;

import bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
public class Flink09_Window_Watermark1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //引入事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取端口数据创建流fei
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //指定数据中的时间字段 指定时间字段 assign有两种API 周期性默认200ms插入一个 与 断点性
        //注意传入参数,最大无序度,如果没有设定waterMark,则默认使用此参数作为waterMark
        SingleOutputStreamOperator<String> watermarks = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });

        //3.压平
        SingleOutputStreamOperator<SensorReading> map = watermarks.map(new MyMapFunc6());

        //4.分组
        KeyedStream<SensorReading, String> keyBy = map.keyBy(SensorReading::getId);

        //5.开窗
        WindowedStream<SensorReading, String, TimeWindow> timeWindow = keyBy.timeWindow(Time.seconds(5),Time.seconds(5));

        //6.聚合操作
        SingleOutputStreamOperator<SensorReading> temp = timeWindow.maxBy("temp");

        //7.打印
        temp.print("最近10秒的最高温度是");

        //8.执行
        env.execute();

    }
    //转换为javaBean
    private static class MyMapFunc6 implements MapFunction<String,SensorReading>{
        @Override
        public SensorReading map(String value) throws Exception {
            String[] fields = value.split(",");
            return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2]));
        }
    }
}
