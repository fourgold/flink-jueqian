package day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class FlinkSQL04_Source_Kafka {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.创建Kafka的连接器
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("test")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "BigData0720"))
                .withFormat(new Csv())
//                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("kafka");

        //3.创建表
        Table table = tableEnv.from("kafka");

        //4.TableAPI
        Table tableResult = table.groupBy("id").select("id,temp.max");

        //5.SQL
        Table sqlResult = tableEnv.sqlQuery("select id,min(temp) from kafka group by id");

        //6.转换为流进行输出
        tableEnv.toRetractStream(tableResult, Row.class).print("Table");
        tableEnv.toRetractStream(sqlResult, Row.class).print("SQL");

        //7.执行
        env.execute();

    }

}
