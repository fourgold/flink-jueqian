package com.atguigu.app;


import com.atguigu.bean.ItemCount;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * 实时热门商品,每隔五分钟输出一个小时内点击量最高的topN
 * 分析: window 1h 5min
 * 一小时内有很多商品,我怎么把商品进行排序
 * 数据ms+定时器
 */
public class HotItemApp2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //拿到业务数据
        DataStreamSource<String> actionSource = env.readTextFile("./input/UserBehavior1.csv");
        SingleOutputStreamOperator<UserBehavior> userRowBehavior = actionSource.map(new MyMapFunc());
        SingleOutputStreamOperator<UserBehavior> userBehavior = userRowBehavior.filter(line -> "pv".equals(line.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp()*1000L;
                    }
                });
        //按照id对pv进行初步分组
        KeyedStream<UserBehavior, Long> keyedUserBehavior = userBehavior.keyBy(UserBehavior::getItemId);

        //分组后进行窗口聚合 windowStream
        WindowedStream<UserBehavior, Long, TimeWindow> windowStream = keyedUserBehavior.timeWindow(Time.minutes(10), Time.minutes(5)).allowedLateness(Time.minutes(5));

        //然后,因为需要一个window里的所有key的集合,所有需要使用aggregate
        //首先我们一个window需要包含所有的(key-点击数),
        // 希望把同一个窗口里面的所有数据放在一起做排序,使用窗口时间作为keyby,窗口结束时间,窗口关闭一毫秒开始计算
        //apply是全量窗口计算,我们希望来一条计算一条
        //aggregate是增量聚合函数,第一个是增量聚合,第二个提取窗口信息,
//        SingleOutputStreamOperator<Long> aggregate = windowStream.aggregate(new ItemCountAggFunc2());
        SingleOutputStreamOperator<ItemCount> aggregate1 = windowStream.aggregate(new ItemCountAggFunc2(), new ItemCountWindowFunc2());
//        aggregate.print("直接聚合:");
        aggregate1.print("聚合之后再使用windowfunc:");

        env.execute();

    }

    //转换为javaBean
    private static class MyMapFunc implements MapFunction<String, UserBehavior> {
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

    //第一步聚合,来一个 聚合一个,在一个窗口内,按照key将数据进行统计
    public static class ItemCountAggFunc2 implements AggregateFunction<UserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            System.out.println(value.getItemId());
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            System.out.println("调用一次merge");
            return a+b;
        }
    }

    //聚合好的一个时间窗口聚合好的数据传入到里面
    public static class ItemCountWindowFunc2 implements WindowFunction<Long, ItemCount,Long,TimeWindow> {

        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            //将每一个窗口里的数据转换按照key转换为一个itemCount对象
            ItemCount itemCount = new ItemCount(aLong, window.getEnd(), input.iterator().next());
            out.collect(itemCount);
        }
    }
}
