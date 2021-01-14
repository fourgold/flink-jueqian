package com.ecust.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author JueQian
 * @create 01-12 17:07
 * 计数滑动窗口
 */
public class Flink07_CountWindow_Tumbling {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x2 读取自定义流
//        DataStreamSource<SensorReading> sensorSource = env.addSource(new Flink05_Source_UDFSource.MySource());
        DataStreamSource<String> sensorSource = env.socketTextStream("hadoop102", 9999);

        //0x3 分组
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = sensorSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = s.split(" ");
                for (String field : fields) {
                    collector.collect(new Tuple2<>(field, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMap.keyBy(0);

        //0x4 开窗 todo 加一个参数变成滑动时间窗口
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.countWindow(10).sum(1);

        //0x5 打印数据
        sum.print();

        //0x6 执行
        env.execute();
    }
}
