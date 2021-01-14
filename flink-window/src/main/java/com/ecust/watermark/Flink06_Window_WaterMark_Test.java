package com.ecust.watermark;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author JueQian
 * @create 01-14 15:17
 */
public class Flink06_Window_WaterMark_Test {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> dataStream = env
                .socketTextStream("hadoop102", 9999)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    // 当前时间戳
                    long currentTimeStamp = 0L;
                    // 允许的迟到数据
                    long maxDelayAllowed = 0L;
                    // 当前水位线
                    long currentWaterMark;
                    @Override
                    public Watermark getCurrentWatermark() {
                        currentWaterMark = currentTimeStamp - maxDelayAllowed;
                        System.out.println("当前水位线:" + currentWaterMark);
                        return new Watermark(currentWaterMark);
                    }
                    @Override
                    public long extractTimestamp(String s, long l) {
                        String[] arr = s.split(",");
                        long timeStamp = Long.parseLong(arr[1]);
                        currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
                        System.out.println("Key:" + arr[0] + ",EventTime:" + timeStamp + ",水位线:" + currentWaterMark);
                        return timeStamp;
                        }
                    });
            dataStream.map(new MapFunction<String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(String s) throws Exception {
                    return new Tuple2<String, String>(s.split(",")[0], s.split(",")[1]);
                }
            }).keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                    .fold("Start:", new FoldFunction<Tuple2<String, String>, String>() {
                        @Override
                        public String fold(String s, Tuple2<String, String> o) throws Exception {
                            return s + " - " + o.f1;
                        }
                    }).print();

            env.execute("MainWaterMark001");
        }
}
