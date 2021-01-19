package com.ecust.checkpoint;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JueQian
 * @create 01-18 21:19
 */
public class Flink02_State_SavePoint {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
    }
}
