package com.ecust.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JueQian
 * @create 01-18 13:26
 * 介绍一下FlinkBackend的配置
 *
 * 问题:bug
 * 1.在hdfs看不到文件
 * 2.RocksDB打断状态问题
 */
public class Flink04_State_StateBackend {
    public static void main(String[] args) throws Exception {

        //0x0 获取配置与状态后端
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //默认checkpoint 间隔时间500ms
        //env.getCheckpointConfig().setCheckpointInterval(500);

        // 默认exactly-once
        //env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);

        //默认状态后端 内存>JVM
        //env.setStateBackend(new MemoryStateBackend());

        //设置HDFS状态后端 = 内存 + hdfs
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkcheckpoints/rocks"));

        //设置RocksDB状态后端 = rocksDB
        //ocksDBStateBackend(String checkpointDataUri, boolean enableIncrementalCheckpointing)

        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flinkcheckpoints/rocks",true));

        //todo 如果开启增量需要将enableCheckPointing关掉
        env.enableCheckpointing(5000L);

        //0x1 处理数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> lastName = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                String[] fields = s.split(",");
                return fields[0];
            }
        }).map(new RichMapFunction<String, String>() {
            private ValueState<String> lastName;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastName = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastName", String.class));
            }

            @Override
            public String map(String s) throws Exception {
                String last = lastName.value();
                String[] fields = s.split(",");
                lastName.update(fields[0]+"_"+last);
                if (last != null) {
                    return fields[0] + "_" + last;
                } else {
                    return fields[0];
                }
            }
        });

        SingleOutputStreamOperator<String> result = lastName;

        //0x2 打印结果
        result.print();

        //0x3 执行
        env.execute();
    }
}
