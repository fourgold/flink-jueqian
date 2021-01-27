package day02;

import bean.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink13_Transform_Reduce {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorReadingDS = socketTextStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        //4.按照传感器ID进行分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorReadingDS.keyBy("id");

        //5.取每个传感器的最高温度
        SingleOutputStreamOperator<SensorReading> result = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(),
                        value2.getTs(),
                        Math.max(value1.getTemp(), value2.getTemp()));
            }
        });

        //6.打印
        result.print();

        //7.开启任务
        env.execute();

    }
}
