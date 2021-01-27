package day05;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink02_State_Backend_CK_Config {

    public static void main(String[] args) throws IOException {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置状态后端
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/flinkCk"));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/flinkCk"));

        //3.CK配置
        //3.1 开启CK
        env.enableCheckpointing(10000L);
        //3.2 设置两次CK开启的间隔时间
        env.getCheckpointConfig().setCheckpointInterval(5000L);
        //3.3 设置CK模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //3.4 设置同时最多有多少个CK任务
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
        //3.5 设置CK超时时间
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        //3.6 CK重试次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        //3.7 两次CK之间的最小间隔时间(头尾)
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        //3.8 如果存在更近的SavePoint,是否采用SavePoint恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);

        //4.重启策略
        //4.1 固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        //4.2 失败率重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(50),Time.seconds(5)));

    }

}
