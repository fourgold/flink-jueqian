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
public class FlinkSQL04_Source_Kafka1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创kafka连接器
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("test")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG,"BigData0720"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING()  )
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafka");

        //创建表
        Table table = tableEnv.from("kafka");

        //TableAPi
        //求最大值
        Table tableResult = table.groupBy("id").select("id,temp.max");

        //SQL
        //求最小值
        Table sqlQuery = tableEnv.sqlQuery("select id,max(temp) from kafka group by id");

        //生产者
        //bin/kafka-console-producer.sh --broker-list hadoop102:9092 --topic test

        //将表转换为流进行输出 最大值应该采用撤回流 getString[] resultSet mysql
        tableEnv.toRetractStream(tableResult, Row.class).print("api");
        tableEnv.toRetractStream(sqlQuery,Row.class).print("sql");

        env.execute();
    }
}
