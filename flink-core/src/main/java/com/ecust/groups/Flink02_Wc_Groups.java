package com.ecust.groups;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author Jinxin Li
 * @create 2021-01-08 8:55
 * 运行共享组的概念:为了保证单slot中的任务链过长对其造成负担
 * 将一个任务链切割在不同的slot中
 * 添加共享组
 * 一共4个共享组,原本,需要1slot,现在需要4个slot
 */
public class Flink02_Wc_Groups {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //0x0 使用工具类从命令行获取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //0x1 获取端口数据
        DataStreamSource<String> portDS = env.socketTextStream(host, port);

        //0x2 处理数据计算wordCount
        SingleOutputStreamOperator<Tuple2<String, Integer>> word2One = portDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = new Tuple2<>(word, 1);
                    collector.collect(tuple2);
                }
            }
        }).slotSharingGroup("group1");

        //0x3 打印数据
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedOne = word2One.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> one2Sum = keyedOne.sum(1).slotSharingGroup("group2");
        one2Sum.print().slotSharingGroup("group3");

        //0x4
        env.execute();

    }
}
