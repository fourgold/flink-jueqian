package day02;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 可以合并成一个任务链(相当于Spark的Stage)的要求:
 * 1.one2one操作
 * 2.并行度相同
 * 3.同一个共享组
 */
public class Flink02_WordCount_Unbounded {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        //4.过滤,不需要Hello数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> filter = wordToOne.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return !"hello".equals(value.f0);
            }
        }).setParallelism(3);

        //5.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = filter.keyBy(0);

        //6.计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //7.打印结果
        result.print();

        //8.开启任务
        env.execute();

    }

}
