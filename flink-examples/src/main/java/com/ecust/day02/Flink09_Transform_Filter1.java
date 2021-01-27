package day02;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink09_Transform_Filter1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> source = env.readTextFile("./sensor");
        SingleOutputStreamOperator<String> filter = source.filter(new MyFilterFunc());

        //3.转换为JavaBean并打印
        filter.print();
        //4.执行任务
        env.execute();


    }

    private static class MyFilterFunc implements org.apache.flink.api.common.functions.FilterFunction<String> {
        @Override
        public boolean filter(String value) throws Exception {

            String[] fields = value.split(",");
            SensorReading sensorReading = new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            if (sensorReading.getTemp()>30){
                return false;
            }else {
                return true;
            }
        }
    }
}
