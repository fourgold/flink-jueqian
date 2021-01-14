package com.ecust.operators;

import com.ecust.beans.SensorReading;
import com.ecust.utils.KafkaTool;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

/**
 * @author Jinxin Li
 * @create 2021-01-09 8:45
 */
public class Flink01_Operator_map {
    public static void main(String[] args) throws Exception {

        //0x1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x2 从kafka读取数据流
        KafkaTool kafkaSource = new KafkaTool("test");
        FlinkKafkaConsumer011<String> kafkaDS = kafkaSource.getKafkaData();
        //问题:并行度与消费者数量有关么?

        DataStreamSource<String> kafka = env.addSource(kafkaDS);

        //0x3 处理数据
        SingleOutputStreamOperator<SensorReading> sensor = kafka.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] words = s.split(",");
                return new SensorReading(words[0], Long.parseLong(words[1]), Double.parseDouble(words[2]));
            }
        });

        //0x4 打印数据
        sensor.print();

        //0x5 执行
        env.execute();
    }
}
