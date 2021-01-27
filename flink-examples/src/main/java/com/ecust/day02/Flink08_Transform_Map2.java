package day02;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_Transform_Map2 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> sensorDS = env.readTextFile("./sensor");

        //3.转换为JavaBean并打印
        sensorDS.map(new MyMapFunc1()).print();

        //4.执行任务
        env.execute();
    }

    private static class MyMapFunc1 implements MapFunction<String,SensorReading> {
        @Override
        public SensorReading map(String value) throws Exception {
            String[] fields = value.split(",");
            SensorReading sensorReading = new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            return sensorReading;
        }
    }
}
