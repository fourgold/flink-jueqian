package com.ecust.source;


import com.ecust.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;


/**
 * @author Jinxin Li
 * @create 2021-01-08 14:27
 * 从集合中读取数据
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 从集合获取数据创建流
        List<SensorReading> list = Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1));

        DataStreamSource<SensorReading> colDS = env.fromCollection(list);

        //0x2 处理数据 过滤一30度以上
        SingleOutputStreamOperator<SensorReading> filterDS = colDS.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading sensorReading) throws Exception {
                if (sensorReading.getTemp() > 30) {
                    return true;
                }
                return false;
            }
        });

        //0x3 打印数据
        filterDS.print();

        //0x4 执行
        env.execute();
    }
}
