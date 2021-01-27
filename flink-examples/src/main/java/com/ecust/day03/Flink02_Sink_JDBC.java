package day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink02_Sink_JDBC {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据写入MySQL
        socketTextStream.addSink(new MyJDBC());

        //4.执行
        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<String> {

        //声明JDBC连接
        private Connection connection;

        //声明预编译SQL
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {

            //获取JDBC连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "000000");

            //预编译SQL
            preparedStatement = connection.prepareStatement("INSERT INTO sensor_id(id,temp) VALUES(?,?) ON DUPLICATE KEY UPDATE temp=?;");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {

            //切割
            String[] fields = value.split(",");

            //给预编译SQL赋值
            preparedStatement.setString(1, fields[0]);
            preparedStatement.setString(2, fields[2]);
            preparedStatement.setString(3, fields[2]);

            //执行
            preparedStatement.execute();

        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }

}
