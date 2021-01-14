package com.ecust.operators;

import com.ecust.beans.SensorReading;
import com.ecust.utils.KafkaTool;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;


/**
 * @author Jinxin Li
 * @create 2021-01-09 15:53
 * reduce算子是将两个元素两两聚合
 */
public class Flink04_Operator_reduce {
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

            //保留当前最高温度,使用当前时间
        SingleOutputStreamOperator<SensorReading> result = idStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                value1.setTs(value2.getTs());
                if (value1.getTemp() < value2.getTemp()) {
                    value1.setTemp(value2.getTemp());
                }
                return value1;
            }
        });

        //0x4 打印数据
        result.print();

        //0x5 执行
        env.execute();
    }
}
