package com.ecust.processapi;

import com.ecust.beans.SensorReading;
import com.ecust.source.Flink05_Source_UDFSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.SimpleFormatter;

/**
 * @author JueQian
 * @create 01-16 9:15
 */
public class Flink01_ProcessAPI_WCTest {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(1);

        //0x1 从自定义数据源中读取数据
        DataStreamSource<SensorReading> source = env.addSource(new Flink05_Source_UDFSource.MySource());

        SingleOutputStreamOperator<SensorReading> streamOperator = source.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SensorReading>() {
            Long currentMaxTimestamp = 0L;
            Long maxOutOfOrderness = 2000L;
            Long lastEmittedWatermark = 0L;

            @Override
            public Watermark getCurrentWatermark() {
                long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
                if (potentialWM >= lastEmittedWatermark) {
                    lastEmittedWatermark = potentialWM;
                }
                return new Watermark(lastEmittedWatermark);
            }

            @Override
            public long extractTimestamp(SensorReading sensorReading, long l) {
                Long timestamp = sensorReading.getTs();
                if (timestamp > currentMaxTimestamp) {
                    currentMaxTimestamp = timestamp;
                    return timestamp;
                }
                return currentMaxTimestamp;
            }
        });

        //0x3 使用processAPI
            //转换为字符串
        SingleOutputStreamOperator<String> process = streamOperator.process(new MyProcessFunc());
            //转换为tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> process1 = process.process(new ProcessMapFunc());
            //开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> tuple2TupleTimeWindowWindowedStream = process1
                .keyBy(0)
                .timeWindow(Time.seconds(10));

        //0x4 最后通过窗口后输出
        tuple2TupleTimeWindowWindowedStream.process(new MyWindowProcess()).print();


        //0x5 执行
        env.execute();

    }

    public static class MyProcessFunc extends ProcessFunction<SensorReading,String>{
        //声明周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            //获取运行时上下文
            RuntimeContext runtimeContext = getRuntimeContext();
//            getRuntimeContext().getState()
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            /*//事件时间的定时器
            ctx.timerService().registerEventTimeTimer(1L);
            ctx.timerService().deleteEventTimeTimer(1L);

            //处理事件的定时器
            ctx.timerService().registerProcessingTimeTimer(1L);
            ctx.timerService().deleteProcessingTimeTimer(1L);*/

            //获取当前waterMark
            long watermark = ctx.timerService().currentWatermark();

            //获取当前处理事间
            long currentProcessingTime = ctx.timerService().currentProcessingTime();

            //获取测输出流 输出watermark
//            ctx.output(new OutputTag<Long>("side"){},watermark);

            //输出
            out.collect(value.getId());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }


    public static class ProcessMapFunc extends ProcessFunction<String, Tuple2<String,Integer>>{

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long watermark = ctx.timerService().currentWatermark();
            String format = simpleDateFormat.format(new Date(watermark));
            System.out.println("元素-"+value+"的watermark:"+format);
            out.collect(new Tuple2<>(value,1));
        }
    }

    public static class MyWindowProcess extends ProcessWindowFunction<Tuple2<String, Integer>,Tuple2<String, Integer>,Tuple,TimeWindow>{

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            int count = 0;
            long watermark = context.currentWatermark();
            String watermarkf = simpleDateFormat.format(new Date(watermark));

            Iterator<Tuple2<String, Integer>> iterator = iterable.iterator();
            while (iterator.hasNext()){
                count = count+iterator.next().f1;
            }
            long start = context.window().getStart();
            String startf = simpleDateFormat.format(new Date(start));
            long end = context.window().getEnd();
            String endf = simpleDateFormat.format(new Date(end));
            collector.collect(new Tuple2<>("水位线:"+watermarkf+",start:"+startf+",end:"+endf+",count:",count));
        }
    }
}
