package day01;

import day02.Flink04_Source_File2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Set;

/**
 * 参数优先级
 * 1.代码单独设置并行度
 * 2.代码全局:env.setParallelism(1);
 * 3.提交任务时参数:命令行(界面)
 * 4.集群配置文件
 */
public class Flink03_WordCount_Unbounded1 {
    static HashMap<String, Integer> hashMap = new HashMap<>();

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.压平
        SingleOutputStreamOperator<String> flatMap = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(s1);
                }
            }
        });

        //4.分组
        SingleOutputStreamOperator<HashMap<String, Integer>> map = flatMap.map(new MyMapFunc2());
        map.print();


        //7.启动任务
        env.execute();

    }
    private static class MyMapFunc2 implements MapFunction<String, HashMap<String, Integer>> {


        @Override
        public HashMap<String, Integer> map(String value) throws Exception {
            Set<String> keySet = hashMap.keySet();
            if (keySet.contains(value)){
                Integer integer = hashMap.get(value);
                hashMap.put(value,integer+1);
            }else {
                hashMap.put(value,1);
            }
            return hashMap;
        }
    }
}
