package day07;

import bean.SensorReading;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * 自定UDF函数求字段的长度
 */
public class FlinkSQL19_Function_UDF1 {

    public static void main(String[] args) throws Exception {

        //1.转换为javaBean三件套
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

        //2.将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS);

        //3.注册函数
        tableEnv.registerFunction("myLen",new MyLen());

        //5.TableAPI 使用UDF
        Table tableApi = table.select("id,id.myLen,temp");

        //6.SQL 方式使用UDF
        Table sqlQuery = tableEnv.sqlQuery("select id,myLen(id),temp from " + table);

        //7.转换为流进行打印数据
        tableEnv.toAppendStream(tableApi,Row.class).print("API");
        tableEnv.toAppendStream(sqlQuery,Row.class).print("SQL");

        //8.执行
        env.execute();

    }

    public static class MyLen extends ScalarFunction {
        public int eval(String word) {
            return word.length();
        }
    }
}
