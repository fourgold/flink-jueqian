package day07;

import bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class FlinkSQL22_Function_UDATF {

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
        tableEnv.registerFunction("top2", new Top2());

        //5.TableAPI 使用UDATF
        Table result = table.groupBy("id")
                .flatAggregate("top2(temp) as (tempa,rank)")
                .select("id,tempa,rank");

        //6.转换为流进行打印数据
        tableEnv.toRetractStream(result, Row.class).print();

        //7.执行
        env.execute();

    }

    public static class Top2 extends TableAggregateFunction<Tuple2<Double, Integer>, Tuple2<Double, Double>> {

        //初始化缓冲
        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<>(Double.MIN_VALUE, Double.MIN_VALUE);
        }

        //来一条数据进行计算的方法
        public void accumulate(Tuple2<Double, Double> acc, Double v) {
            //跟最大值进行比较
            if (v > acc.f0) {
                acc.f1 = acc.f0;
                acc.f0 = v;
            } else if (v > acc.f1) {
                acc.f1 = v;
            }
        }

        //发送数据的方法
        public void emitValue(Tuple2<Double, Double> acc, Collector<Tuple2<Double, Integer>> out) {

            out.collect(new Tuple2<>(acc.f0, 1));

            if (acc.f1 != Double.MIN_VALUE) {
                out.collect(new Tuple2<>(acc.f1, 2));
            }
        }

    }

}
