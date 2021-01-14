package com.ecust.sink;

import com.ecust.beans.SensorReading;
import com.ecust.source.Flink05_Source_UDFSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author Jinxin Li
 * @create 01-12 7:38
 */
public class Flink05_Sink_JDBC {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 使用mysource作为数据源
        DataStreamSource<SensorReading> sensor = env.addSource(new Flink05_Source_UDFSource.MySource());

        //0x2 写入MySQL数据库
        sensor.addSink(new MyJDBCSinkFunc());

        //0x3 执行环境
        env.execute();

    }

    public static class MyJDBCSinkFunc extends RichSinkFunction<SensorReading> {
        Connection connection = null;
        PreparedStatement insertStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");

            // 创建预编译器，有占位符，可传入参数
            insertStmt = connection.prepareStatement("INSERT INTO sensor (id,ts,temp) VALUES (?,?,?)");

        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            System.out.println(value.getId());
            insertStmt.setString(1,value.getId());
            insertStmt.setLong(2,value.getTs());
            insertStmt.setDouble(3,value.getTemp());

            //执行sql语句
            insertStmt.execute();
        }

        @Override
        public void close() throws Exception {
            connection.close();
            insertStmt.close();
        }
    }
}
