package day02;

import bean.SensorReading;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class Flink14_Transform_Split_Select1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorReadingDS = socketTextStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        //4.按照温度大小进行切分
        SplitStream<SensorReading> split = sensorReadingDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        //5.使用Select选择流
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");
        DataStream<SensorReading> all = split.select("low", "high");


        //6.打印
        high.print("high");
        low.print("low");
        all.print("all");


        //7.开启任务
        env.execute();

    }
}
