package day06;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.elasticsearch.common.recycler.Recycler;

public class FlinkSQL07_Sink_ES1 {

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

        //3.Table API
        Table table = tableEnv.fromDataStream(sensorDS);
        Table tableResult = table.select("id,temp");

        //5.SQL API
        tableEnv.createTemporaryView("sensor", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");

        //6.创建ES连接器
        Elasticsearch elasticsearch = new Elasticsearch()
                .version("6")
                .host("hadoop102", 9200, "http")
                .index("sensor")
                .documentType("_doc")
                .keyDelimiter("-")
                .bulkFlushMaxActions(1);
        Schema schema = new Schema().field("id", DataTypes.STRING()).field("temp", DataTypes.DOUBLE());
        ConnectTableDescriptor descriptor = tableEnv.connect(elasticsearch).withFormat(new Json()).withSchema(schema).inAppendMode();
        descriptor.createTemporaryTable("sensorTable");

        //7.插入到es
        tableEnv.insertInto("sensorTable",sqlResult);

        //8.执行
        env.execute();

    }

}
