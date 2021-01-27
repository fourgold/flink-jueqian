package practice;

import day04.Flink08_ProcessAPI_WordCount;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author Jinxin Li
 * @create 2020-12-17 15:21
 * 使用FlinkSQL实现从Kafka读取数据计算WordCount并将数据写入ES中
 */
public class Flink06_SQL_Kakfa_WordCount_ES1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("test")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "BigDataTest"))
                .withFormat(new Csv())
//                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("word", DataTypes.STRING())
                        .field("count", DataTypes.DOUBLE()))
                .createTemporaryTable("kafka");

        //3.创建表
        Table table = tableEnv.from("kafka");

        //4.TableAPI
        Table tableResult = table.select("*");


        //6.转换为流进行输出
        tableEnv.toRetractStream(tableResult, Row.class).print("Table");

        //7.转换为流输出
        tableEnv.insertInto("Es", tableResult);

        env.execute();


    }
}
