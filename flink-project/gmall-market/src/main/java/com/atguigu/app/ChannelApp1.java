package com.atguigu.app;

import com.atguigu.bean.ChannelBehaviorCount;
import com.atguigu.bean.MarketUserBehavior;
import com.atguigu.source.MarketBehaviorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.security.spec.EncodedKeySpec;
import java.sql.Timestamp;

//需求 : 每隔5秒钟统计最近一个小时按照 渠道 的推广量。
public class ChannelApp1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从自定义数据源读取数据,并转换为JavaBean
        DataStreamSource<MarketUserBehavior> marketUserBehaviorDS = env.addSource(new MarketBehaviorSource());

        //3.按照渠道和行为进行分组
        KeyedStream<MarketUserBehavior, Tuple> keyedStream = marketUserBehaviorDS.keyBy("channel", "behavior");

        //4.开窗,滑动窗口,滑动步长5秒钟,窗口大小一个小时
        WindowedStream<MarketUserBehavior, Tuple, TimeWindow> timeWindow = keyedStream.timeWindow(Time.hours(1), Time.seconds(5));

        //5.使用aggregate实现累加聚合以及添加窗口信息的功能
        SingleOutputStreamOperator<ChannelBehaviorCount> aggregate = timeWindow.aggregate(new MyAgg(), new MyWindow());

        //6.打印结果
        aggregate.print();

        //7.执行
        env.execute();
    }

    public static class MyAgg implements AggregateFunction<MarketUserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }
        @Override
        public Long add(MarketUserBehavior value, Long accumulator) {
            return accumulator+1L;
        }
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }
        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    public static class MyWindow implements WindowFunction<Long, ChannelBehaviorCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelBehaviorCount> out) throws Exception {
            long end = window.getEnd();
            String endTime = new Timestamp(end).toString();
            ChannelBehaviorCount count = new ChannelBehaviorCount(tuple.getField(0), tuple.getField(1), endTime, input.iterator().next());
            out.collect(count);
        }
    }
}
