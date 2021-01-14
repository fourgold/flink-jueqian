package com.ecust.wc;

import com.ecust.function.Function01_MyFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jinxin Li
 * @create 2021-01-06 16:27
 */
public class Flink02_Wc_BoundedStream {
    public static void main(String[] args) throws Exception {

        //0x0 创建执行环境 使用StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //0x1 从文件中读取数据
        DataStreamSource<String> fileDS = env.readTextFile("./data/word.txt");

        //0x2 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> word2One = fileDS.flatMap(new Function01_MyFlatMapFunction());
                                                                // 流处理需要使用keyBy进行分流
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedOne = word2One.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedOne.sum(1);

        //0x3 打印
        result.print();

        //0x4 执行
        env.execute();
    }
}
