package com.ecust.watermark;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * @author JueQian
 * @create 01-14 15:17
 * 这个程序主要根据源码重写waterMark方法
 */
public class Flink07_Window_WaterMark_MyTest {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //0x1 设定事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //0x2 设置生成watermark的周期
        env.getConfig().setAutoWatermarkInterval(2000);

        //0x3 获取nc数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //0x4 指定事件时间字段
        SingleOutputStreamOperator<String> watermarks = socketTextStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
            long currentTimeStamp = 0L;//用于存储当前元素的时间语义

            //允许迟到的数据
            long maxDelayAllowed = 2000L;
            //当前水位线
            long currentWaterMark;

            private long lastEmittedWatermark = -9223372036854775808L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                long potentialWM;//定义判断水位线

                potentialWM = currentTimeStamp - maxDelayAllowed;
                //保证waterMark自增
                if (potentialWM >= lastEmittedWatermark) {
                    lastEmittedWatermark = potentialWM;
                }
                System.out.println("当前水位线:" + lastEmittedWatermark);
                return new Watermark(lastEmittedWatermark);
            }

            @Override
            public long extractTimestamp(String s, long l) {
                String[] fields = s.split(",");
                long timeStamp = Long.parseLong(fields[1])*1000L;
                //取最大的作为当前时间戳
                currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
                System.out.println("key:" + fields[0] + ",EventTime:" + timeStamp + ",水位线:" + currentWaterMark);
                return timeStamp;
            }
        });

        //0x4 转换为键值对
        SingleOutputStreamOperator<Tuple2<String, Integer>> string2One = watermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple2<>(fields[0],1);
            }
        });

        //0x5 分组开窗处理数据
        string2One.keyBy(0).timeWindow(Time.seconds(5)).sum(1).print(">>>>>>>>>>>>输出数据>>>>>>>>>");

        //0x6 执行数据
        env.execute();
    }
}
