package practice;


import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Properties;

// 从Kafka读取传感器数据
// 统计每个传感器发送温度的次数存入MySQL(a表)
// 如果某个传感器温度连续10秒不下降
// 则输出报警信息到侧输出流并存入MySQL(b表)
public class Flink05_WordCount_OnTimer_SideOutPut_State_JdbcSink2 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        source.addSink(new MyJDBCSink2());
        source.print();
        env.execute();
    }

    private static class MyJDBCSink2 extends RichSinkFunction<String> {
        private Connection connection;
        private PreparedStatement preparedStatement;
        private String sqlSide = "insert into sensor_alert (ts,id,inter,temp) values(?,?,?,?)";


        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            preparedStatement = connection.prepareStatement(sqlSide);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
                String[] fields = value.split(",");
            preparedStatement.setString(1,fields[0]);
            preparedStatement.setString(2,fields[1]);
            preparedStatement.setString(3,fields[2]);
            preparedStatement.setString(4,fields[3]);
                System.out.println("side------------------------");
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
