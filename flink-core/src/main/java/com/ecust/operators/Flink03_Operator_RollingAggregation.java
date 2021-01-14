package com.ecust.operators;

import com.ecust.beans.SensorReading;
import com.ecust.utils.KafkaTool;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;


/**
 * @author Jinxin Li
 * @create 2021-01-09 14:47
 * max取当前属性的最大值
 * maxBy是相关于其他属性
 */
public class Flink03_Operator_RollingAggregation {
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

            //max 仅仅会改变温度
            // 1> SensorReading(id="sensor_10", ts=1547718205, temp=38.1)
            //1> SensorReading(id="sensor_10", ts=1547718205, temp=38.2)
            //1> SensorReading(id="sensor_10", ts=1547718205, temp=50.0)
        //SingleOutputStreamOperator<SensorReading> temp = idStream.max("temp");

            //maxBy会一直发生改变,其他属性也会参加改变
        SingleOutputStreamOperator<SensorReading> temp = idStream.maxBy("temp");

        //todo 希望温度用最新的温度,时间用最新的时间 应该采用reduce

        //0x4 打印数据
        temp.print();

        //0x5 执行
        env.execute();
    }
}
