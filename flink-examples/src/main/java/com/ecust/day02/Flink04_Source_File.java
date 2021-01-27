package day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_Source_File {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

        //3.打印
        sensorDS.print();

        //4.执行任务
        env.execute();

    }
}
