package com.ecust.source;

import com.ecust.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jinxin Li
 * @create 2021-01-08 16:45
 * 从kafka中读取数据
 */
public class Flink03_Source_Port {
    public static void main(String[] args) throws Exception {

        //0x0 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 获取Kafka数据源
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        DataStreamSource<String> portDS = env.socketTextStream(host, port);

        //0x2 数据处理
        SingleOutputStreamOperator<SensorReading> s2Sensor = portDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        SingleOutputStreamOperator<SensorReading> filterDS = s2Sensor.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading sensorReading) throws Exception {
                return sensorReading.getTemp() > 30;
            }
        });

        //0x3 打印结果
        filterDS.print();

        //0x4 执行环境
        env.execute();
    }
}
