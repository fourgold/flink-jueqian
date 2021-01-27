package day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_WordCount_Bounded {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文本数据创建流
        DataStreamSource<String> input = env.readTextFile("input");

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //5.计算WordCount
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //6.打印
        result.print();

        //7.启动任务
        env.execute("Flink02_WordCount_Bounded");

    }

}
