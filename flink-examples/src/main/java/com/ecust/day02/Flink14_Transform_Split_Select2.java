package day02;

import bean.SensorReading;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class Flink14_Transform_Split_Select2 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据创建流
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunc3());

        //4.按照温度大小进行切分
        SplitStream<SensorReading> splitStream = map.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30 ? Collections.singletonList("highTempDStream") : Collections.singletonList("lowTempDStream");
            }
        });

        //5.使用Select选择流
        DataStream<SensorReading> highTempDStream = splitStream.select("highTempDStream");
        DataStream<SensorReading> lowTempDStream = splitStream.select("lowTempDStream");
        DataStream<SensorReading> all = splitStream.select("highTempDStream", "lowTempDStream");

        //6.打印
        highTempDStream.print("highTempDStream");
        lowTempDStream.print("lowTempDStream");
        all.print("all");

        //7.开启任务
        env.execute();
    }

    private static class MyMapFunc3 implements MapFunction<String,SensorReading> {
        @Override
        public SensorReading map(String value) throws Exception {
            String[] fields = value.split(",");
            SensorReading sensorReading = new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            return sensorReading;
        }
    }
}
