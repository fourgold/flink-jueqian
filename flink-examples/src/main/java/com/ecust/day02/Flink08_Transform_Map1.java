package day02;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_Transform_Map1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> sensorDS = env.readTextFile("./sensor");

        //3.转换为JavaBean并打印
        sensorDS.map(value -> {
            String[] strings = value.split(",");
            return new SensorReading(strings[0],Long.parseLong(strings[1]),Double.parseDouble(strings[2]));
        }).print();

        //4.执行任务
        env.execute();
    }

}
