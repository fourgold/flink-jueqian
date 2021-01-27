package com.atguigu.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jinxin Li
 * @create 2020-12-19 14:15
 */
public class HotUrlApp1 {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //2.读取文本数据创建流

        //3.按照url分组

        //4.开床,滑动窗口,窗口大小10分钟,滑动步长5秒钟,允许处理一秒的迟到数据

        //5.计算每个窗口内部每个URL的访问次数 滚动聚合

        //6.按照窗口信息重新分组

        //7.是用processFunction处理排序 状态编程,定时器


    }
}

