package com.ecust.operators;

import com.ecust.utils.KafkaTool;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

/**
 * @author Jinxin Li
 * @create 2021-01-09 10:47
 */
public class Flink02_Operator_flatMap {
    public static void main(String[] args) throws Exception {
        //0x1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //0x2 从kafka读取数据流
        KafkaTool kafkaSource = new KafkaTool("testTopic1");
        FlinkKafkaConsumer011<String> kafkaDS = kafkaSource.getKafkaData();
        //问题:并行度与消费者数量有关么?

        //设定起始编译量 默认
        kafkaDS.setStartFromGroupOffsets();
        DataStreamSource<String> kafka = env.addSource(kafkaDS);

        //0x3 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> one2Tuple = kafka.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(",");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = one2Tuple.keyBy(0).sum(1);

        //0x4 打印数据
        result.print();

        //0x5 执行
        env.execute();
    }
}
