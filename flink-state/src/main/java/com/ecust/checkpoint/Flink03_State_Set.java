package com.ecust.checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author JueQian
 * @create 01-19 9:07
 */
public class Flink03_State_Set {
    public static void main(String[] args) throws IOException {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //0x1 设置状态后端
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/flinkCk"));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/flinkCK"));

        //0x2 设置CK
        env.enableCheckpointing(10000L);
            //设置ck模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            //两次ck时间间隔
        env.getCheckpointConfig().setCheckpointInterval(500L);
            //设置ck超时时间
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
            //设置同时最多有多少个CK任务
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
            //设置两个任务的最小间隔时间 头跟尾
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
            //ck重试次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
            //设置是够倾向于使用checkPoint回复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);

        //0x3 重启策略 固定延迟 重启三次. 5秒重启一次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
            //失败率重启策略 每隔50秒内重启3次, 每次与每次之间的间隔3秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(50),Time.seconds(3)));
    }
}
