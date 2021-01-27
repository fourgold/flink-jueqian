package day02;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class Flink15_Transform_Connect_CoMap1 {

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

        //5.使用Select选择流,将两个流变成不同得对象
        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowDStream = splitStream.select("low");

        // todo 将高流转换为tuple2
        SingleOutputStreamOperator<Tuple2<String, String>> highMapTuple2 = highStream.map(new MyMapFunc4());

        //6.连接两条流
        ConnectedStreams<Tuple2<String, String>, SensorReading> connect = highMapTuple2.connect(lowDStream);

        /*DataStream<SensorReading> union = highStream.union(lowDStream);
        union.print();*/
        //7.合并两条流,需要将两条流的内部的对象变成一种对象
        SingleOutputStreamOperator<Tuple2<String, String>> connectDStream = connect.map(new CoMapFunction<Tuple2<String, String>, SensorReading, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map1(Tuple2<String, String> value) throws Exception {
                return value;
            }

            @Override
            public Tuple2<String, String> map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemp().toString() + "修改过");
            }
        });


        //7.打印数据
        connectDStream.print();

        //8.开启任务
        env.execute();

    }

    private static class MyMapFunc4 implements MapFunction<SensorReading,Tuple2<String,String>> {

        @Override
        public Tuple2<String, String> map(SensorReading value) throws Exception {
            return new Tuple2<String,String>(value.getId(),value.getTemp().toString());
        }
    }
}
