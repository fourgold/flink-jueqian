package com.atguigu.app;

import com.atguigu.bean.ItemCount;
import com.atguigu.bean.ItemCount1;
import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UserBehavior1;
import kafka.server.DynamicConfig;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * 实时热门商品,每隔五分钟输出一个小时内点击量最高的topN
 * 分析: window 1h 5min
 * 一小时内有很多商品,我怎么把商品进行排序
 * 数据ms+定时器
 */
public class HotItemApp1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //定义时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean,同时提取数据中的时间戳生成Watermark
        DataStreamSource<String> source = env.readTextFile("./data/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> userAction = source.map(new MyMapFunc11());
        //过滤
        SingleOutputStreamOperator<UserBehavior> pvAction = userAction.filter(line -> line.getBehavior().equals("pv"));
        //指定时间
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = pvAction.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        //3.按照商品id分组
        KeyedStream<UserBehavior, Long> keyedStream = userBehaviorDS.keyBy(UserBehavior::getItemId);

        //4.开窗,滑动窗口,一个小时窗口大小,5分钟滑动步长
        WindowedStream<UserBehavior, Long, TimeWindow> windowedStream = keyedStream.timeWindow(Time.hours(1), Time.minutes(5));

        //5.聚合计算,计算窗口内部每个商品被点击的次数(滚动聚合保证来一条计算一条,窗口函数保证可以拿到窗口时间)
//        windowedStream.apply() 回头复习 windowStream的apply方法
        //这个函数可以前面做增量,后面提取窗口信息,来一套聚合一条,窗口计算(滚动聚合,全量窗口)
        //对每个窗口里进行累加计算
        //要获取一个窗口内的所有对象,同时为对象添加窗口结束时间
        SingleOutputStreamOperator<ItemCount1> itemCount = windowedStream.aggregate(new ItemCountAggFunc1(), new ItemCountWindowFunc1());

        //6.按照窗口时间分组
        KeyedStream<ItemCount1, Long> windowEndKeyedStream = itemCount.keyBy(ItemCount1::getWindowEnd);

        //7.使用ProcessFunction实现收集每个窗口中的数据做排序输出(状态编程--ListState  定时器)
        SingleOutputStreamOperator<String> process = windowEndKeyedStream.process(new ItemKeyedProcessFunc1(5));

        //8.打印输出
        process.print();

        //9.执行
        env.execute();
    }

    //转换成JavaBean对象
    private static class MyMapFunc11 implements MapFunction<String, UserBehavior>{
        @Override
        public UserBehavior map(String value) throws Exception {
            String[] fields = value.split(",");
            UserBehavior behavior = new UserBehavior(Long.parseLong(fields[0]),
                    Long.parseLong(fields[1]),
                    Integer.parseInt(fields[2]),
                    fields[3],
                    Long.parseLong(fields[4]));
            return behavior;
        }
    }

    //使用这个滚动聚合,保证来一条计算一条,只需要Long,缓冲为Long,输出结果为Long 计算浏览了相同商品用了多少条
    public static class ItemCountAggFunc1 implements AggregateFunction<UserBehavior, Long, Long> {
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
    //两个分区的相加
        @Override
        public Long merge(Long a, Long b) {
            System.out.println("调用一次merge");
            return a+b;
        }
    }

    //用来做窗口聚合,先聚合函数做预计算,window的输入,是聚合的输出,输出是应该是itemId,count,window_end
    public static class ItemCountWindowFunc1 implements WindowFunction<Long, ItemCount1,Long,TimeWindow>{
        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemCount1> out) throws Exception {
            //获取key
            //获取窗口结束时间
            //获取当前窗口中当前item的id的点击次数
            out.collect(new ItemCount1(aLong,window.getEnd(),input.iterator().next()));
            //当前迭代器里只有一条数据
        }
    }
    //使用ProcessFunction实现收集每个窗口中的数据做排序输出(状态编程--ListState  定时器)
    public static class ItemKeyedProcessFunc1 extends KeyedProcessFunction<Long,ItemCount1,String> {
        //定义属性
        private int topSize;
        private ListState<ItemCount1> listState;

        public ItemKeyedProcessFunc1() {
        }

        public ItemKeyedProcessFunc1(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount1>("item", ItemCount1.class));
        }

        @Override
        public void processElement(ItemCount1 value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<ItemCount1> iterator = listState.get().iterator();
            ArrayList<ItemCount1> itemCount1s = Lists.newArrayList(iterator);

            itemCount1s.sort(new Comparator<ItemCount1>() {
                @Override
                public int compare(ItemCount1 o1, ItemCount1 o2) {
                    return (int)(o2.getCount()-o1.getCount());
                }
            });

            //输出TopSize数据
            StringBuilder sb = new StringBuilder();
            sb.append("=============")
                    .append(new Timestamp(timestamp - 1000L))
                    .append("=============")
                    .append("\n");

            //遍历排序之后的结果
            for (int i = 0; i < Math.min(topSize, itemCount1s.size()); i++) {
                //a.提取数据
                ItemCount1 itemCount = itemCount1s.get(i);

                //b.封装Top数据
                sb.append("Top").append(i + 1);
                sb.append(" ItemId:").append(itemCount.getItemId());
                sb.append(" Count:").append(itemCount.getCount());
                sb.append("\n");
            }

            //休息
            Thread.sleep(2000);

            //输出数据
            out.collect(sb.toString());
        }
    }
}
