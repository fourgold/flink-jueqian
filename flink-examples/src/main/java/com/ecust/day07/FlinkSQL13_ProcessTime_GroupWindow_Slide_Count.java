package day07;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL13_ProcessTime_GroupWindow_Slide_Count {

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

        //3.将流转换为表并指定处理时间字段
        Table table = tableEnv.fromDataStream(sensorDS, "id,ts,temp,pt.proctime");

        //4.基于时间的滚动窗口TableAPI
        Table tableResult = table.window(Slide.over("5.rows").every("2.rows").on("pt").as("sw"))
                .groupBy("sw,id")
                .select("id,id.count");

        //5.转换为流进行输出
        tableEnv.toAppendStream(tableResult, Row.class).print("Table");

        //7.执行任务
        env.execute();


    }
}
