package com.ecust.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Jinxin Li
 * @create 2021-01-06 16:30
 */
//0x2x1 创建一个FlatMapFunction
public class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>> {

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        //1.切分
        String[] words = s.split(" ");
        //2.转换成二元组
        for (String word : words){
            Tuple2<String, Integer> tuple = new Tuple2<>(word, 1);
            //往下游发送数据
            collector.collect(tuple);
        }
    }
}
