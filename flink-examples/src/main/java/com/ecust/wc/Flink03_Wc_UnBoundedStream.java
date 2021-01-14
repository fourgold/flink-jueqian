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
 * @create 2021-01-06 16:41
 */
public class Flink03_Wc_UnBoundedStream {
    public static void main(String[] args) throws Exception {

        //0x1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //0x2 从socket中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        //0x3 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> word2One = socketDS.flatMap(new Function01_MyFlatMapFunction());
                    //注意返回泛型,是<Tuple2<String, Integer>, Tuple>,第二个tuple是封装的key类型,因为keyBy()可以传可变形参
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedOne = word2One.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedOne.sum(1);

        //0x4 打印
        result.print();

        //0x5 启动
        env.execute();
    }
}
