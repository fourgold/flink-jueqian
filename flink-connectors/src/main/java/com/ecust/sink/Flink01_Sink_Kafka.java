package com.ecust.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author Jinxin Li
 * @create 2021-01-11 10:30
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 获取kafka数据源
        /*KafkaTool test = new KafkaTool("test");
        FlinkKafkaConsumer011<String> kafkaData = test.getKafkaData();*/
        //0x1 获取文件的数据源
        DataStreamSource<String> source1 = env.readTextFile("./data/sensor.txt");

        //0x2 处理kafka数据
//        DataStreamSource<String> source = env.addSource(kafkaData);
        SingleOutputStreamOperator<String> words = source1.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] words = s.split(",");
                return words[0] + " " + words[1] + " " + words[2];
            }
        });

        //0x3 将数据输出到kafka
        words.print();
        words.addSink(new FlinkKafkaProducer011<String>("hadoop102:9092", "test", new SimpleStringSchema()));
        //0x4 执行
        env.execute();
    }
}
