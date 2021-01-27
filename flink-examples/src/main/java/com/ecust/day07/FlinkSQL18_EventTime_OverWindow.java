package day07;

import bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL18_EventTime_OverWindow {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                    @Override
                    public long extractAscendingTimestamp(SensorReading element) {
                        return element.getTs();
                    }
                });

        //3.将流转换为表并指定处理时间字段
        Table table = tableEnv.fromDataStream(sensorDS, "id,ts,temp,rt.rowtime");

        //4.TableAPI
        Table tableResult = table.window(Over.partitionBy("id").orderBy("rt").as("ow"))
                .select("id,id.count over ow");

        //5.SQL
        tableEnv.createTemporaryView("sensor", table);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) over(partition by id order by rt) ct " +
                "from sensor");

        //6.转换为流进行打印输出
        tableEnv.toAppendStream(tableResult, Row.class).print("Table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");

        //7.执行
        env.execute();

    }
}
