package day07;

import bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class FlinkSQL20_Function_UDTF {

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

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS);

        //4.注册函数
        tableEnv.registerFunction("split", new Split());

        //5.TableAPI 使用UDTF   sensor_1  =>  [sensor 6,1 1]
        Table tableResult = table
                .joinLateral("split(id) as (word,len)")
                .select("id,word,len");

        //6.SQL 方式使用UDTF
        Table sqlResult = tableEnv.sqlQuery("select id,word,len from " +
                table +
                ",lateral table(split(id)) as splitTable(word,len)");

        //7.转换为流进行打印数据
        tableEnv.toAppendStream(tableResult, Row.class).print("Table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");

        //8.执行
        env.execute();

    }

    public static class Split extends TableFunction<Tuple2<String, Integer>> {

        public void eval(String value) {

            String[] fields = value.split("_");

            for (String field : fields) {
                collect(new Tuple2<>(field, field.length()));
            }

        }

    }


}
