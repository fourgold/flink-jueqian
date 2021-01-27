package day07;

import bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * 需求,求平均温度
 */
public class FlinkSQL21_Function_UDAF1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境并转换为表
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
        tableEnv.registerFunction("tempAvg", new TempAvg1());

        //5.TableAPI 使用UDAF
        Table tableApi = table.groupBy("id").select("id,temp.tempAvg");


        //6.SQL 方式使用UDAF
        Table sqlQuery = tableEnv.sqlQuery("select id,tempAvg(temp) from " + table +" group by id");


        //7.转换为流进行打印数据
        tableEnv.toRetractStream(tableApi,Row.class).print("api");
        tableEnv.toRetractStream(sqlQuery,Row.class).print("sql");


        //8.执行
        env.execute();

    }

    //acc主要是一个记录器
    public static class TempAvg1 extends AggregateFunction<Double,Tuple2<Double,Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0/accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<Double,Integer>(0D,0);
        }

        public void accumulate(Tuple2<Double,Integer> acc,Double value){
            acc.f1=acc.f1+1;
            acc.f0=acc.f0+value;
        }
    }
}
