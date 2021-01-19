package com.ecust.processapi;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author JueQian
 * @create 01-16 15:11
 * 每条数据进来之后两秒之后往流中放入一个时间戳
 */
public class Flink02_ProcessAPI_OnTimer {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 从端口获取数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        KeyedStream<String, String> keyedStream = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        });

        //0x2 定时两秒打印一个数据
        keyedStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime+2000L);
                System.out.println("定一个两秒后的闹钟:"+(currentProcessingTime+2000L));
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("闹钟响了"+ctx.timerService().currentProcessingTime());
            }
        });

        //0x3 执行
        env.execute();
    }
}
