package com.ecust.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Jinxin Li
 * @create 2021-01-08 16:39
 * 从文件中读取数据
 * 一行一行的读取
 */
public class Flink02_Source_TextFile {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //0x1 读取文件
        DataStreamSource<String> fileDS = env.readTextFile("./data/word.txt");

        //0x2 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = fileDS.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word,1));
                }
            }
        }).keyBy(0).sum(1);

        //0x3 打印数据
        result.print();

        //0x4 执行环境
        env.execute();
    }
}
