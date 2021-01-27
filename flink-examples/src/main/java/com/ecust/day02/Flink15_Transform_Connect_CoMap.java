package day02;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class Flink15_Transform_Connect_CoMap {

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
        SingleOutputStreamOperator<Tuple2<String, Double>> highDS = splitStream
                .select("high")
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                         @Override
                         public Tuple2<String, Double> map(SensorReading value) throws Exception {
                             return new Tuple2<>(value.getId(), value.getTemp());
                         }
                     }
                );

        DataStream<SensorReading> lowDS = splitStream.select("low");

        //6.连接两条流
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = highDS.connect(lowDS);

        //7.合并两条流
        SingleOutputStreamOperator<Object> result = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {

            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return value;
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return value;
            }
        });

        //7.打印数据
        result.print();

        //8.开启任务
        env.execute();

    }
}
