package day02;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

import java.util.Collections;

public class Flink16_Transform_Union1 {

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

        //4.按照温度大小进行切分
        SplitStream<SensorReading> splitStream = sensorReadingDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30.0 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        //5.使用Select选择流
        DataStream<SensorReading> highDS = splitStream.select("high");
        DataStream<SensorReading> lowDS = splitStream.select("low");

        SingleOutputStreamOperator<Tuple2<String, String>> highDS2Tuple= highDS.map(new MyMapFunc5());


        //6.合并两个流打印
        DataStream<SensorReading> allDS = lowDS.union(highDS);
        allDS.print();

        //7.开启任务
        env.execute();

    }

    private static class MyMapFunc5 implements MapFunction<SensorReading, Tuple2<String,String>> {
        @Override
        public Tuple2<String, String> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(),value.getTemp().toString()+"tuple类型");
        }
    }
}
