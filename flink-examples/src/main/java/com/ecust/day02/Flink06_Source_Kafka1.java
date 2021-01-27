package day02;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Flink06_Source_Kafka1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从Kafka读取数据创建流
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Bigdata0720");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>("test",
                new SimpleStringSchema(),
                properties));

        //3.打印
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = source.flatMap(new Flink04_Source_File2.MyFlatMapFunc1());

        flatMap.keyBy(0).sum(1).print();

        //4.执行任务
        env.execute();

    }
}
