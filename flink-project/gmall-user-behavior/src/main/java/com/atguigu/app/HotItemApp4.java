package com.atguigu.app;

import com.atguigu.bean.ItemCount;
import com.atguigu.bean.ItemCount1;
import com.atguigu.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.bind.ValidationEvent;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * 实时热门商品,每隔五分钟输出一个小时内点击量最高的topN
 * 分析: window 1h 5min
 * 一小时内有很多商品,我怎么把商品进行排序
 * 数据ms+定时器
 */
public class HotItemApp4 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        //3.按照商品id分组
        KeyedStream<UserBehavior, Long> keyedStream = userBehaviorDS.keyBy(UserBehavior::getItemId);

        //4.开窗,滑动窗口,一个小时窗口大小,5分钟滑动步长
        WindowedStream<UserBehavior, Long, TimeWindow> windowedStream = keyedStream.timeWindow(Time.hours(1), Time.minutes(5));

        //5.首先在窗口内进行聚合计算 todo aggregate的理解
        SingleOutputStreamOperator<ItemCount> aggregate = windowedStream.aggregate(new ItemAggreFunc(), new ItemWindowFunc());

        //6.按照窗口时间分组
        SingleOutputStreamOperator<String> result = aggregate.keyBy(ItemCount::getWindowEnd).process(new MyProcessFunc(5));

        //8.打印输出
        result.print();

        //9.执行
        env.execute();

    }

    private static class ItemAggreFunc implements AggregateFunction<UserBehavior, Long, Long> {
       //使用新的累加器
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        //累加一次
        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator+=1L;
        }

        //返回结果
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

       // merge不同slot
        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    private static class ItemWindowFunc implements WindowFunction<Long, ItemCount, Long, TimeWindow> {

        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception{
            out.collect(new ItemCount(aLong,window.getEnd(),input.iterator().next()));
        }
    }

    //把窗口数据进行打上标记
    private static class MyProcessFunc extends KeyedProcessFunction<Long, ItemCount,String>{
        private int topSize;

        //todo 构造器
        public MyProcessFunc(int topSize) {
            this.topSize = topSize;
        }

        //定义列表状态
        private ListState<ItemCount> itemList;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemList = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("itemList", ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            itemList.add(value);
            //获取window结束的时间'
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<ItemCount> iterator = itemList.get().iterator();
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);

            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    return (int)(o2.getCount()-o1.getCount());
                }
            });
            StringBuilder builder = new StringBuilder();
            builder.append("=============")
                    .append(new Timestamp(timestamp - 1000L))
                    .append("=============")
                    .append("\n");
            for (int i = 0; i <  Math.min(topSize, itemCounts.size()); i++) {
                ItemCount itemCount = itemCounts.get(i);
                builder.append("top"+i+">>>>>>>>>>>>"+itemCount+"\n");
            }
            Thread.sleep(5000L);

            //清空状态
            itemList.clear();
            out.collect(builder.toString());
        }
    }
}