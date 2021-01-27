package com.atguigu.app;

import com.atguigu.bean.ApacheLog;
import com.atguigu.bean.UrlCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class HotUrlApp {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean,过滤,提取数据中的时间戳生成watermark
//        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.readTextFile("input/apache.log")
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, ApacheLog>() {
                    @Override
                    public ApacheLog map(String value) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        String[] fields = value.split(" ");
                        return new ApacheLog(fields[0],
                                fields[1],
                                sdf.parse(fields[3]).getTime(),
                                fields[5],
                                fields[6]);
                    }
                })
                .filter(data -> "GET".equals(data.getMethod()))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(2)) {

                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                });

        //3.按照URL进行分组
        KeyedStream<ApacheLog, String> urlKeyedStream = apacheLogDS.keyBy(ApacheLog::getUrl);

        //4.开窗,滑动窗口,窗口大小10分钟,滑动步长5秒钟,允许处理1分钟的迟到数据
        WindowedStream<ApacheLog, String, TimeWindow> windowedStream = urlKeyedStream.timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.seconds(60));

        //5.计算每个窗口内部每个URL的访问次数,滚动聚合,WindowFunction提取窗口信息
        SingleOutputStreamOperator<UrlCount> urlCountDS = windowedStream.aggregate(new UrlCountAggFunc(), new UrlCountWindowFunc());

        //6.按照窗口信息重新分组
        KeyedStream<UrlCount, Long> windowEndKeyedStream = urlCountDS.keyBy(UrlCount::getWindowEnd);

        //7.使用ProcessFunction处理排序,状态编程  定时器
        SingleOutputStreamOperator<String> result = windowEndKeyedStream.process(new UrlCountProcessFunc(5));

        //8.打印输出结果
        apacheLogDS.print("apacheLog");
        urlCountDS.print("agg");
        result.print("result");

        //9.执行
        env.execute();

    }

    public static class UrlCountAggFunc implements AggregateFunction<ApacheLog, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLog value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class UrlCountWindowFunc implements WindowFunction<Long, UrlCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<UrlCount> out) throws Exception {
            out.collect(new UrlCount(url, window.getEnd(), input.iterator().next()));
        }
    }


    public static class UrlCountProcessFunc extends KeyedProcessFunction<Long, UrlCount, String> {

        //定义属性
        private int topSize;

        public UrlCountProcessFunc() {
        }

        public UrlCountProcessFunc(int topSize) {
            this.topSize = topSize;
        }

        //声明集合状态
        private ListState<UrlCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<UrlCount>("list-state", UrlCount.class));
        }

        @Override
        public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {

            //进来数据则加入状态
            listState.add(value);

            //注册1毫秒后的定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

            //注册1分钟后的定时器,用于清空状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            if (timestamp == ctx.getCurrentKey() + 60000L) {
                //清空状态
                listState.clear();
                //返回
                return;
            }

            //取出状态中的数据
            Iterator<UrlCount> iterator = listState.get().iterator();

            ArrayList<UrlCount> urlCounts = Lists.newArrayList(iterator);

            //排序
            urlCounts.sort(new Comparator<UrlCount>() {
                @Override
                public int compare(UrlCount o1, UrlCount o2) {
                    if (o1.getCount() > o2.getCount()) {
                        return -1;
                    } else if (o1.getCount() < o2.getCount()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            //准备输出数据
            StringBuilder sb = new StringBuilder();
            sb.append("===========").append(new Timestamp(timestamp - 1)).append("===========").append("\n");
            //遍历数据取出TopN
            for (int i = 0; i < Math.min(topSize, urlCounts.size()); i++) {

                //取出单条数据
                UrlCount urlCount = urlCounts.get(i);
                sb.append("Top ").append(i + 1);
                sb.append(" URL:").append(urlCount.getUrl());
                sb.append(" Count:").append(urlCount.getCount());
                sb.append("\n");
            }

            //休息
            Thread.sleep(100);

            //清空状态
            //listState.clear();

            //输出数据
            out.collect(sb.toString());

        }
    }
}
