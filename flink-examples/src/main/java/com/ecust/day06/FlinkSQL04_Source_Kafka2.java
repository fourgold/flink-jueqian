package day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @author Jinxin Li
 * @create 2020-12-16 14:50
 * oldCSV是flink自己开发的
 */

public class FlinkSQL04_Source_Kafka2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("test")
                .property("bootstrap.connect","hadoop102:9092")
                );


        //







        //执行环境
        env.execute();
    }
}
