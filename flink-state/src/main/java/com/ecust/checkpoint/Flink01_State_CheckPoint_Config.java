package com.ecust.checkpoint;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author JueQian
 * @create 01-18 13:26
 *
 * 配置checkpoint
 */
public class Flink01_State_CheckPoint_Config {
    public static void main(String[] args) throws Exception {

        //0x0 获取配置与状态后端
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        //todo 设置精准一次性
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //todo checkPoint的超时时间,超过这个时间就不做了
        env.getCheckpointConfig().setCheckpointTimeout(60*1000L);
        //异步的状态是copy一下,然后用副本去发送,写入,然后继续并行

        //todo 默认进行的checkpoint的同时进行的数量2个
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        //todo 至少有一些执行任务的间隔时间 最小的间隔时间 slide的参数
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2);

        //todo 是否倾向于checkPoint用于恢复 默认是false 有问题??????????????
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        //todo 是否失败次数 容忍三次失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        //todo 设置重启策略 重启次数,重启时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        //todo 设置时间间隔
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5,Time.of(5, TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)));

        //默认状态后端 内存 JVM
//        env.setStateBackend(new MemoryStateBackend());


        //设置HDFS状态后端 + 内存 RocksDB
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkcheckpoints/rocks"));

        //设置RocksDB
        //ocksDBStateBackend(String checkpointDataUri, boolean enableIncrementalCheckpointing)
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flinkcheckpoints/rocks",true));

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
