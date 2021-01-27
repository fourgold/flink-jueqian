package day02;

import bean.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class Flink14_Transform_Split_Select {

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
        SplitStream<SensorReading> splitStream = sensorReadingDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30.0 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        //5.使用Select选择流
        DataStream<SensorReading> highDS = splitStream.select("high");
        DataStream<SensorReading> lowDS = splitStream.select("low");
        DataStream<SensorReading> allDS = splitStream.select("high", "low");

        //6.打印
        highDS.print("high:");
        lowDS.print("low:");
        allDS.print("all:");

        //7.开启任务
        env.execute();

    }
}
