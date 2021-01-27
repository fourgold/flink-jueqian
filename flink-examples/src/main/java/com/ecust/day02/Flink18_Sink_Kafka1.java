package day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Flink18_Sink_Kafka1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> sensor = env.socketTextStream("hadoop102", 9999);

        //3.将数据写入Kafka
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");

        sensor.addSink(new FlinkKafkaProducer011<String>("test",new SimpleStringSchema(),properties));


        //4.执行任务
        env.execute();

    }
}
