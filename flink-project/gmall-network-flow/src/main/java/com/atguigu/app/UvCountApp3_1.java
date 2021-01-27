package com.atguigu.app;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UvCountTs;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class UvCountApp3_1 {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean,同时提取数据中的时间戳生成Watermark
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("data/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new UserBehavior(Long.parseLong(fields[0]),
                                Long.parseLong(fields[1]),
                                Integer.parseInt(fields[2]),
                                fields[3],
                                Long.parseLong(fields[4]));
                    }
                })
                .filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.开窗
        WindowedStream<UserBehavior, Long, TimeWindow> windowedStream = userBehaviorDS.keyBy(UserBehavior::getUserId).timeWindow(Time.hours(1));

        //4.这里不使用全量窗口函数
        KeyedStream<UvCountTs, String> keyedStream = windowedStream.aggregate(new myAggreFunc(), new MyWindowdFunc()).keyBy(UvCountTs::getTime);

        //5.打印数据
//        result.print();
        SingleOutputStreamOperator<UvCountTs> result = keyedStream.process(new MyKeyedProcess());

        result.print();

        //6.执行
        env.execute();

    }

    private static class myAggreFunc implements AggregateFunction<UserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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

    private static class MyWindowdFunc implements WindowFunction<Long,UvCountTs,Long,TimeWindow> {
        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<UvCountTs> out) throws Exception {
            Long end = window.getEnd();
            out.collect(new UvCountTs(end.toString(),input.iterator().next()));
        }
    }

    private static class MyKeyedProcess extends KeyedProcessFunction<String, UvCountTs, UvCountTs> {

        private ValueState<UvCountTs> uvCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            uvCount = getRuntimeContext().getState(new ValueStateDescriptor<UvCountTs>("value", UvCountTs.class));
        }

        @Override
        public void processElement(UvCountTs value, Context ctx, Collector<UvCountTs> out) throws Exception {
            UvCountTs lastUvCount = uvCount.value();
            if (lastUvCount==null){
                uvCount.update(new UvCountTs(new Timestamp(Long.parseLong(ctx.getCurrentKey())).toString(),1L));
            }else {
                lastUvCount.setCount(lastUvCount.getCount()+1L);
            }
            ctx.timerService().registerEventTimeTimer(Long.parseLong(ctx.getCurrentKey())+1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UvCountTs> out) throws Exception {
            if (uvCount.value()!=null){
                out.collect(uvCount.value());
            }
            uvCount.clear();
        }
    }
}