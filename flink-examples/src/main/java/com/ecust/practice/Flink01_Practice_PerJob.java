package com.ecust.practice;

import com.ecust.beans.SensorReading;
import com.ecust.source.Flink05_Source_UDFSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author Jinxin Li
 * @create 2021-01-11 14:58
 * 画出yarn-perjob模式下的提交流程图
 */
public class Flink01_Practice_PerJob {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //0x1 读取MySource数据
        DataStreamSource<SensorReading> source = env.addSource(new Flink05_Source_UDFSource.MySource());

        //0x2 处理数据换成自己字符串
        SingleOutputStreamOperator<String> one2String = source.map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading sensorReading) throws Exception {
                return sensorReading.getId() + "-" + sensorReading.getTs() + "-" + sensorReading.getTemp();
            }
        });

        //0x3 使用kafka的source
        one2String.addSink(new FlinkKafkaProducer011<String>("hadoop102:9092","test",new SimpleStringSchema()));

        //0x4 执行
        env.execute();
    }
}
