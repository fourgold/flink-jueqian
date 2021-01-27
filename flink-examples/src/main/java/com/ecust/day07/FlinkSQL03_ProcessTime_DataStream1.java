package day07;

import bean.SensorReading;
import day02.Flink08_Transform_Map;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author Jinxin Li
 * @create 2020-12-18 10:13
 */
public class FlinkSQL03_ProcessTime_DataStream1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> sensorOpt
                = source.map(new Flink08_Transform_Map.MyMapFunc());

        //将流转换为表并指定处理时间字段 额外声明处理时间字段
        tableEnv.fromDataStream(sensorOpt,"id,ts.proctime");


    }
}
