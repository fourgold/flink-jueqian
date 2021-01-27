package day03;

import com.mysql.jdbc.JDBC4Connection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink02_Sink_JDBC1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据写入MySQL
        socketTextStream.addSink(new MyJDBC1());

        //4.执行
        env.execute();
    }

    private static class MyJDBC1 extends RichSinkFunction<String> {

        //声明JDBC连接
        private Connection connection;

        //声明预编译SQL
        private PreparedStatement preparedStatement;
        @Override
        public void open(Configuration parameters) throws Exception {

            //获得JDBC连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            preparedStatement = connection.prepareStatement("INSERT INTO sensor_id(id,temp) VALUES(?,?) ON DUPLICATE KEY UPDATE temp=?;");
        }
        //处理数据
        @Override
        public void invoke(String value, Context context) throws Exception {
            String[] fields = value.split(",");

            //给预编译sql赋值
            preparedStatement.setString(1,fields[0]);
            preparedStatement.setString(2,fields[2]);
            preparedStatement.setString(3,fields[2]);

            //执行
            preparedStatement.execute();

        }
        @Override
        public void close() throws Exception {
            super.close();
        }

    }
}
