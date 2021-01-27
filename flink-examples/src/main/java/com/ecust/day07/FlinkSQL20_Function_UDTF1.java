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

/**
 * 需求:将对象的id.按照_进行炸开
 * 函数会将一个str:a_b_c,1,2
 * 炸成str:
 * a_b_c,a,1,2
 * a_b_c,b,1,2
 * a_b_c,c,1,2
 * 总结:加一列,多三行 类似于
 * a_b_c,1,2 a,b,c==>()
 * 是固定一边的笛卡尔积
 * tuple里面是行啊
 */
public class FlinkSQL20_Function_UDTF1 {

    public static void main(String[] args) throws Exception {

        //1.获取数据转换为表
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
        Table table = tableEnv.fromDataStream(sensorDS);

        //4.注册函数
        tableEnv.registerFunction("split", new Split());
        //5.TableAPI
        Table apiQuery = table.joinLateral("split(id) as (word1,word2)").select("id,word1,word2");
        //6.SQL 方式使用UDTF
        Table sqlQuery = tableEnv.sqlQuery("select id,word1,word2 from " + table + ", Lateral table(split(id)) as T(word1,word2)");
        //7.转换为流进行打印数据
        tableEnv.toAppendStream(apiQuery,Row.class).print("tableApi");
        tableEnv.toAppendStream(sqlQuery,Row.class).print("sql");
        //8.执行
        env.execute();
    }
    public static class Split extends TableFunction<Tuple2<String,String>> {
        public void eval(String str){
            String[] s = str.split("_");

            collect(new Tuple2<>(s[0],s[1]));
            //答案是这样的 首先是tableFunction,不是scalarFunction!!
            //tuple2,是变成两个column,而普通的collect(),一个就代表一列,调用一次就是一列
            //内部是笛卡尔积的运算
            //如(a_b,1)*(a,b)=>(a_b,a,1)(a_b,b,1)变成了两行,这样子是使用下面程序
            //for (String s1 : s) {
            //                collect(s1);
            //            } 当然泛型得改一下
            //如果想变成一行
            //就得使用tuple,
            //(a_b,1)*tuple(a,b)=>(a_b,a,b,1)
        }
    }
}
