package com.ecust.operators;

import com.ecust.beans.SensorReading;
import com.ecust.utils.KafkaTool;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * @author Jinxin Li
 * @create 2021-01-09 16:03
 * split是将一个流拆分成两个流,但是其本身还是一个大流,需要使用select将其分开
 * dataStream->SplitStream.select->dataStream
 *
 */
public class Flink05_Operator_splitAndSelect {
    public static void main(String[] args) throws Exception {
        //0x1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //0x2 从kafka读取数据流
        KafkaTool kafkaSource = new KafkaTool("testTopic1");
        FlinkKafkaConsumer011<String> kafkaDS = kafkaSource.getKafkaData();

        //设定起始编译量 默认
        kafkaDS.setStartFromGroupOffsets();
        DataStreamSource<String> kafka = env.addSource(kafkaDS);

        //0x3 处理数据
        SingleOutputStreamOperator<SensorReading> string2Sensor = kafka.flatMap(new FlatMapFunction<String, SensorReading>() {
            @Override
            public void flatMap(String s, Collector<SensorReading> collector) throws Exception {
                String[] words = s.split(",");
                collector.collect(new SensorReading(words[0], Long.parseLong(words[1]), Double.parseDouble(words[2])));

            }
        });
        //根据id进行分组
        KeyedStream<SensorReading, String> idStream = string2Sensor.keyBy(SensorReading::getId);

        //分流,将高于30度的分流
        SplitStream<SensorReading> splitStream = idStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemp() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> high = splitStream.select("high");
        DataStream<SensorReading> low = splitStream.select("low");

        //0x4 打印数据
        high.print("high");
        low.print("low");

        //0x5 执行
        env.execute();
    }
}
