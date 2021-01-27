package day05;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jinxin Li
 * @create 2020-12-15 16:24
 */
public class Flink02_State_Backend_CK_Config1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
//        env.setStateBackend(new Rock)

        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().setCheckpointInterval(500L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置同时拥有多少ck业务
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);

        //设置ck超时时间
        env.getCheckpointConfig().setCheckpointTimeout(1000L);

        //ck重试次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        //第一个
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);

        //是否从savePoint回复

        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);

        //固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(3)));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.seconds(50),Time.seconds(3)));


    }
}
