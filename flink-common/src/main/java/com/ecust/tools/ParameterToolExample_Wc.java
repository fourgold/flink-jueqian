package com.ecust.tools;

import com.ecust.function.MyFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jinxin Li
 * @create 2021-01-06 20:12
 */
public class ParameterToolExample_Wc {
    public static void main(String[] args) throws Exception {

        //0x0 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //0x1 从端口中获取数据
            //DataStreamSource<String> portDS = env.socketTextStream("hadoop102", 9999);

            //工具类需要传入参数 --host hadoop102 --port 9999
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> portDS = env.socketTextStream(host, port);

        //0x2 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> word2One = portDS.flatMap(new MyFlatMapFunction());
        KeyedStream<Tuple2<String, Integer>, Tuple> wordToOne = word2One.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordToOne.sum(1);

        //0x3 打印数据
        result.print();

        //0x4 启动
        env.execute();
    }
}
