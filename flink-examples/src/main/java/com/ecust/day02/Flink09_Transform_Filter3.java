package day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink09_Transform_Filter3 {

    public static void main(String[] args) throws Exception {

        HashSet<String> set = new HashSet<>();

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> sensorDS = env.readTextFile("input");

        SingleOutputStreamOperator<String> flatMap = sensorDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(s1);
                }
            }
        });

        flatMap.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (set.contains(value)){
                    return false;
                }else {
                    set.add(value);
                    return true;
                }
            }
        }).print();

        //4.执行任务
        env.execute();

    }
}
