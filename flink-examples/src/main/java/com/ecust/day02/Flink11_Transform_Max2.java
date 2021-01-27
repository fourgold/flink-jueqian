package day02;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * max是你取那个字段,就给你把某一个字段变成最大的
 * 比如对象的温度属性,max只会改变你一个属性
 */
public class Flink11_Transform_Max2 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //4.按照传感器ID进行分组
        SingleOutputStreamOperator<SensorReading> source2Sensor = source.map(new MyMapFunc3());
        KeyedStream<SensorReading, Tuple> idDS = source2Sensor.keyBy("id");

        //5.取每个传感器的最高温度
        SingleOutputStreamOperator<SensorReading> tempDS = idDS.max("temp");

        //6.打印
        tempDS.print();

        //7.开启任务
        env.execute();

    }

    private static class MyMapFunc3 implements MapFunction<String,SensorReading> {
        @Override
        public SensorReading map(String value) throws Exception {
            String[] fields = value.split(",");
            SensorReading sensorReadingObject = new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            return sensorReadingObject;
        }
    }
}
