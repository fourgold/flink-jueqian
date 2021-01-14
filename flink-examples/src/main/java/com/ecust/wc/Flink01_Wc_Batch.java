package com.ecust.wc;

import com.ecust.function.Function01_MyFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Jinxin Li
 * Flink支持流处理,也支持批处理
 * 批处理WordCount(文件)
 * 学习从这里开始-->
 */
public class Flink01_Wc_Batch {
    public static void main(String[] args) throws Exception {
        //0x0 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //0x1 从文件中读取数据
        DataSource<String> fileDS = env.readTextFile("./data/word.txt");

        //0x2 处理数据 需要传入方程式 javaAPI更贴合框架
        FlatMapOperator<String, Tuple2<String, Integer>> word2One = fileDS.flatMap(new Function01_MyFlatMapFunction());

        UnsortedGrouping<Tuple2<String, Integer>> one2Group = word2One.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> result = one2Group.sum(1);

        //0x3 打印数据
        result.print();

        //0x4 启动

    }
}
