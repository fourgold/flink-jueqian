package day06;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class FlinkSQL08_Sink_ES_Upsert {

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
        Table tableResult = table.groupBy("id,ts").select("id,ts,temp.max");

        //5.SQL
        tableEnv.createTemporaryView("sensor", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,min(temp) from sensor group by id");

        //6.创建ES连接器
        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("hadoop102", 9200, "http")
                .index("sensor4")
                .documentType("_doc")
                .bulkFlushMaxActions(1))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .inUpsertMode()
                .createTemporaryTable("Es");

        //7.将数据写入文件系统
        tableEnv.insertInto("Es", tableResult);
//        tableEnv.insertInto("Es", sqlResult);
//        tableEnv.toRetractStream(tableResult, Row.class).print();

        //8.执行
        env.execute();

    }

}
