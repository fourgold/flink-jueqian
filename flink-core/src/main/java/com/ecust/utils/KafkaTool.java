package com.ecust.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.common.internals.Topic;

import java.util.Properties;

/**
 * @author Jinxin Li
 * @create 2021-01-09 16:08
 */
public class KafkaTool {
    private Properties properties;
    private String topic;
    private FlinkKafkaConsumer011<String> kafkaDS;

    public KafkaTool(String topic) {
        properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "testTopic1");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        this.properties = properties;
        this.topic=topic;
    }

    //不能设置静态让类直接调用,因为需要使用外部参数,才能调用
    public FlinkKafkaConsumer011<String> getKafkaData(){
        kafkaDS = new FlinkKafkaConsumer011<>(topic,
                new SimpleStringSchema(),
                this.properties);
        return kafkaDS;
    }
}
