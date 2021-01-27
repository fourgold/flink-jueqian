package day02;

import bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink11_Transform_Max1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> map = source.map(line -> {
            String[] words = line.split(",");
            return new SensorReading(words[0], Long.parseLong(words[1]), Double.parseDouble(words[2]));
        });

        //4.按照传感器ID进行分组
        KeyedStream<SensorReading, Tuple> id = map.keyBy("id");

        //5.取每个传感器的最高温度
        SingleOutputStreamOperator<SensorReading> temp = id.max("temp");

        //6.打印
        temp.print();

        //7.开启任务
        env.execute();

    }
}
