package day04;

import bean.SensorReading;
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
public class Flink06_Window_Watermark_Trans2 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //引入事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置生成Watermark的周期
        env.getConfig().setAutoWatermarkInterval(500);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTs() * 1000L;
            }
        });

        //4.分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");

        //5.开窗
        WindowedStream<SensorReading, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));

        //6.聚合操作
        SingleOutputStreamOperator<SensorReading> result = windowedStream.sum("temp");

        //7.打印
        result.print();

        //8.执行
        env.execute();

    }
}
