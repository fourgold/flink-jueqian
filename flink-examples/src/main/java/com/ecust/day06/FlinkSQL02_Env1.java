package day06;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author Jinxin Li
 * @create 2020-12-16 14:17
 */
public class FlinkSQL02_Env1 {
    public static void main(String[] args) {
        //使用旧版本,使用BLink需要导入依赖

        //旧版本
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner()      // 使用老版本planner
                .inStreamingMode()    // 流处理模式
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);


        //新版本
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()//有界流
                .build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);

    }
}
