package day06;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL01_Test {

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
        tableEnv.createTemporaryView("sensor", sensorDS);

        //4.SQL方式实现查询
//        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor where id ='sensor_1'");
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) from sensor group by id");

        //5.TableAPI的方式
        Table table = tableEnv.fromDataStream(sensorDS);
//        Table tableResult = table.select("id,temp").where("id = 'sensor_1'");
        Table tableResult = table.groupBy("id").select("id,id.count");

        //6.将表转换为流进行输出
//        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");
//        tableEnv.toAppendStream(tableResult, Row.class).print("Table");

        tableEnv.toRetractStream(sqlResult, Row.class).print("SQL");
        tableEnv.toRetractStream(tableResult, Row.class).print("Table");

        //执行
        env.execute();

    }

}
