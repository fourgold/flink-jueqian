package com.ecust.rich;

import com.ecust.beans.SensorReading;
import com.ecust.utils.KafkaTool;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

/**
 * @author Jinxin Li
 * @create 2021-01-09 17:31
 */
public class Flink01_Rich_FlatMapWc {
    public static void main(String[] args) throws Exception {
        //0x1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x2 从kafka读取数据流
        KafkaTool kafkaSource = new KafkaTool("testTopic1");
        FlinkKafkaConsumer011<String> kafkaDS = kafkaSource.getKafkaData();
        //问题:并行度与消费者数量有关么?

        DataStreamSource<String> kafka = env.addSource(kafkaDS);

        //0x3 处理数据
        SingleOutputStreamOperator<SensorReading> sensor = kafka.map(new MyRichMapFunction());

        //0x4 打印数据
        sensor.print();

        //0x5 执行
        env.execute();

    }

    public static class MyRichMapFunction extends RichMapFunction<String,SensorReading>{
        //生命周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("获取连接");
        }

        @Override
        public SensorReading map(String s) throws Exception {
            String[] fields = s.split(",");
            return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭连接");
        }
    }
}
