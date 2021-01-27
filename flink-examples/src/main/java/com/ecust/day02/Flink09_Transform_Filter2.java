package day02;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink09_Transform_Filter2 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> source = env.readTextFile("./sensor");
        SingleOutputStreamOperator<String> filter = source.filter(line->{
            String[] fields = line.split(",");
            return Double.parseDouble(fields[2])<30;
        });

        //3.转换为JavaBean并打印
        filter.print();
        //4.执行任务
        env.execute();


    }
}
