package day06;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author Jinxin Li
 * @create 2020-12-16 16:26
 */
public class FlinkSQL06_Sink_Kafka1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });

        //将流中的数据转换为表
        Table table = tableEnv.fromDataStream(sensorDS);
        Table selectQuery = table.select("id,temp");

        Kafka kafkaProperties = new Kafka()
                .version("0.11")
                .topic("test")
                .property("bootstrap.servers", "hadoop102:9092")
                .property("group.id", "bigData0720");

        //创建kafka连接
        tableEnv.connect(kafkaProperties).withFormat(new Json()).withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("temp",DataTypes.DOUBLE())).createTemporaryTable("sensor");

        tableEnv.insertInto("sensor",selectQuery);

        env.execute();

    }
}
