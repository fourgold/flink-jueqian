package com.ecust.day02;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Flink03_Source_Collection1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从集合获取数据创建流
        DataStreamSource<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("sensor1", System.currentTimeMillis(), 36.7),
                new SensorReading("sensor1", System.currentTimeMillis(), 35.7),
                new SensorReading("sensor1", System.currentTimeMillis(), 37.7),
                new SensorReading("sensor1", System.currentTimeMillis(), 39.7)
        ));

        //3.打印数据
        source.print();
        //4.启动任务

        env.execute();

    }

}
