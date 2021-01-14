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
 * @create 2021-01-09 17:11
 * 可以直接将同类型的流合并在一起
 */
public class Flink07_Operator_union {
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
            //3.1根据id进行分组
        KeyedStream<SensorReading, String> idStream = string2Sensor.keyBy(SensorReading::getId);

            //3.2分流,根据温度是否大于30度分成高低流
        SplitStream<SensorReading> splitStream = idStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemp() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
            //3.3分流,给分开得流起名字
        DataStream<SensorReading> high = splitStream.select("high");
        DataStream<SensorReading> low = splitStream.select("low");

        //0x4 union直接合并两个流
        DataStream<SensorReading> union = high.union(low);

        //0x5 打印数据
        union.print();

        //0x6 执行
        env.execute();
    }
}
