package com.ecust.source;

import com.ecust.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author Jinxin Li
 * @create 2021-01-08 16:45
 * 从kafka中读取数据
 */
public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {

        //0x0 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 获取Kafka数据源
            // kafka配置项
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

            // 从kafka读取数据 要使用通用的方式,需要额外的jar包flink-connector-kafka-0.11_2.12
            // kafka - RichParallelSourceFunction 可以并行
        DataStreamSource<String> kafkaDS = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "test",
                        new SimpleStringSchema(),
                        properties)).setParallelism(2);
            //设置并行度为2测试并行能力
        SingleOutputStreamOperator<SensorReading> result = kafkaDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        //0x2 打印数据
        result.print();

        //0x3 执行环境
        env.execute();

        //0x4 开启kafka生产者
        //bin/kafka-console-producer.sh --broker-list hadoop102:9092 --topic test

    }
}
