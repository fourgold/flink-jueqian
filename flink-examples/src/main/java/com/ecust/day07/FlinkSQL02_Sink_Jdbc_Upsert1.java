package day07;

import bean.SensorReading;
import day02.Flink08_Transform_Map;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author Jinxin Li
 * @create 2020-12-18 9:21
 */
public class FlinkSQL02_Sink_Jdbc_Upsert1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> sensorOpt
                = source.map(new Flink08_Transform_Map.MyMapFunc());

        Table table = tableEnv.fromDataStream(sensorOpt);
        Table sqlQuery = tableEnv.sqlQuery("select id,temp from" + table);


        String sinkDDL="";
        tableEnv.sqlUpdate(sinkDDL);
        //7.将数据写入MySQL

        env.execute();
    }
}
