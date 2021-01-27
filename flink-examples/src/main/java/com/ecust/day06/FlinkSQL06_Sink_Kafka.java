package day06;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.producer.ProducerConfig;

public class FlinkSQL06_Sink_Kafka {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });

        //3.对流进行注册
        Table table = tableEnv.fromDataStream(sensorDS);

        //4.TableAPI
        Table tableResult = table.select("id,temp");

        //5.SQL
        tableEnv.createTemporaryView("sensor", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");

        //6.创建Kafka连接器
        tableEnv.connect(new Kafka()
                .topic("test")
                .version("0.11")
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092"))
                //.withFormat(new Json())
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaOut");

        //7.将数据写入文件系统
        tableEnv.insertInto("kafkaOut", tableResult);
        tableEnv.insertInto("kafkaOut", sqlResult);

        //8.执行
        env.execute();

    }

}
