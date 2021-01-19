package com.ecust.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author JueQian
 * @create 01-19 14:30
 */
public class Flink05_State_RocksDB {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/ck"));
        env.enableCheckpointing(5000L);

        //设置不自动删除CK
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //0x1 读取端口数据
        SingleOutputStreamOperator<String> source = env.socketTextStream("hadoop102", 9999).uid("source-1");

        //0x2 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> process = source.process(new MyProcessMap1()).uid("process-2");
        SingleOutputStreamOperator<Tuple2<String, Integer>> result0 = process.keyBy(0).sum(1).uid("sum-3");

        //0x2 update 升级处理数据
        SingleOutputStreamOperator<String> result = (SingleOutputStreamOperator<String>) result0.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0 + "总数为:" + stringIntegerTuple2.f1;
            }
        }).uid("mapper-4");

        //0x3 打印数据
        result.print();

        //0x4 执行环境
        env.execute();
    }

    public static class MyProcessMap1 extends ProcessFunction<String, Tuple2<String,Integer>>{
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] fields = value.split(",");
            for (String field : fields) {
                out.collect(new Tuple2<>(field,1));
            }
        }
    }
}
