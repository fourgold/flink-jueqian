package com.atguigu.app;

import com.atguigu.bean.AdClickEvent;
import com.atguigu.bean.AdCountByProvince;
import com.atguigu.bean.BlackListWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * 页面广告点击量
 * 主函数中先以province进行keyBy，然后开一小时的时间窗口，滑动距离为5秒，统计窗口内的点击事件数量。
 * 所以我们可以对一段时间内（比如一天内）的用户点击行为进行约束，
 * 如果对同一个广告点击超过一定限额（比如100次），应该把该用户加入黑名单并报警，此后其点击行为不应该再统计。
 */
public class AdClickApp1 {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流并转换为JavaBean,同时提取时间戳生成Watermark
        DataStreamSource<String> source = env.readTextFile("input/AdClickLog.csv");
        SingleOutputStreamOperator<AdClickEvent> eventSource = source.map(new MyMap());
        SingleOutputStreamOperator<AdClickEvent> event = eventSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        //3.分组按照userID与广告
        KeyedStream<AdClickEvent, Tuple> keyedStream = event.keyBy("userId", "adId");

        //添加过滤逻辑,单日某个人点击某个广告达到100次,加入黑名单(今天后续不再统计该用户的点击信息)
        //统计广告达到100次,加入黑名单,但是每天一清(定时器)
        SingleOutputStreamOperator<AdClickEvent> process = keyedStream.process(new MyProcess());

        //3.按照省份分组
        KeyedStream<AdClickEvent, String> province = process.keyBy(AdClickEvent::getProvince);

        //4.开窗
        WindowedStream<AdClickEvent, String, TimeWindow> timeWindow = province.timeWindow(Time.hours(1), Time.seconds(5));

        //5.使用Aggregate方式实现
        SingleOutputStreamOperator<AdCountByProvince> aggregate = timeWindow.aggregate(new MyAgg1(), new MyWin1());

        //6.打印数据
        aggregate.print("main");
        process.getSideOutput(new OutputTag<BlackListWarning>("BlackList") {}).print("side");

        //7.执行
        env.execute();


    }


    //将string对象转换为javaBean
    public static class MyMap implements MapFunction<String, AdClickEvent> {
        @Override
        public AdClickEvent map(String value) throws Exception {
            String[] fields = value.split(",");
            long userId = Long.parseLong(fields[0]);
            long adId = Long.parseLong(fields[1]);
            String province = fields[2];
            String city = fields[3];
            long timestamp = Long.parseLong(fields[4]);
            return new AdClickEvent(userId,adId,province,city,timestamp);
        }
    }

    //处理黑名单,
    public static class MyProcess extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {

        //此用户在此广告的期间 判断一下 是否已经被处理
        private ValueState<Boolean> isSendState;
        private  ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-send", Boolean.class));
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            //获取所有的userID-adId的点击事件
            Boolean isSend = isSendState.value();
            Long count = countState.value();

            if (count == null){
                //第一条注册定时器
                countState.update(1L);
                long timing = (value.getTimestamp() / (60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000L) - 8 * 60 * 60 * 1000L;
                System.out.println(new Timestamp(timing));
                ctx.timerService().registerEventTimeTimer(timing);
            }else {
                //更新状态
                countState.update(count+1);
                if (count+1L >= 100){
                    if (isSend == null){
                        System.out.println("测输出流");
                        ctx.output(new OutputTag<BlackListWarning>("BlackList") {
                        }, new BlackListWarning(value.getUserId(), value.getAdId(), "拉入黑名单！！！"));
//                        ctx.output(new OutputTag<BlackListWarning>("warning"){},new BlackListWarning(value.getUserId(),value.getAdId(),"该用户连续当日点击广告超过100次"));
                        isSendState.update(true);
                    }
                    return;
                }
            }
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            countState.clear();
            isSendState.clear();
        }
    }

    public static class MyAgg1 implements AggregateFunction<AdClickEvent,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }
        @Override
        public Long add(AdClickEvent value, Long accumulator) {
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

    public static class MyWin1 implements WindowFunction<Long, AdCountByProvince, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            AdCountByProvince adCountByProvince = new AdCountByProvince(s, windowEnd, input.iterator().next());
            out.collect(adCountByProvince);
        }
    }
}
