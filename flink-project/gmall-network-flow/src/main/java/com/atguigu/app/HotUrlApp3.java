package com.atguigu.app;

import com.atguigu.bean.ApacheLog;
import com.atguigu.bean.UrlCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
import java.util.Map;

/**
 * HotUrl按照事件时间的乱序处理
 * 每隔5秒,输出最近10分钟的访问量最多的前N个URL
 */
public class HotUrlApp3 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean,过滤,提取数据中的时间戳生成watermark
//        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.readTextFile("data/apache.log")
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
                //todo 指定乱序时间戳
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(2)) {

                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                });

        //3.按照URL进行分组
        KeyedStream<ApacheLog, String> urlKeyedStream = apacheLogDS.keyBy(ApacheLog::getUrl);

        //4.开窗,滑动窗口,窗口大小10分钟,滑动步长5秒钟,允许处理1分钟的迟到数据 todo 对于乱序数据,由于要求实时性,需要把延迟数据降低,然后提高关窗时间
        WindowedStream<ApacheLog, String, TimeWindow> windowedStream = urlKeyedStream.timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.seconds(60));

        //5.计算每个窗口内部每个URL的访问次数,滚动聚合,WindowFunction提取窗口信息 todo windowfunction可以提取窗口信息
        SingleOutputStreamOperator<UrlCount> aggregate = windowedStream.aggregate(new UrlCountAggreFunc(), new UrlCountsWindowFunc());
//        aggregate.print();
        //6.按照窗口信息重新分组
        KeyedStream<UrlCount, Long> keyedStream = aggregate.keyBy(UrlCount::getWindowEnd);

        //7.使用ProcessFunction处理排序,状态编程  定时器
        SingleOutputStreamOperator<String> process = keyedStream.process(new UrlProcessFunc(5));

        //8.打印输出结果
        process.print();

        //9.执行
        env.execute();

    }

    private static class UrlCountAggreFunc implements AggregateFunction<ApacheLog,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLog value, Long accumulator) {
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

    private static class UrlCountsWindowFunc implements WindowFunction<Long,UrlCount,String,TimeWindow>{
        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<UrlCount> out) throws Exception {
//            System.out.println("经过windowfunction");
            out.collect(new UrlCount(url, window.getEnd(), input.iterator().next()));
        }
    }

    private static class UrlProcessFunc extends KeyedProcessFunction<Long,UrlCount,String> {
        private int topSize;
        MapState<String, UrlCount> mapState;

        public UrlProcessFunc(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState  = getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlCount>("url", String.class, UrlCount.class));
        }

        @Override
        public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
            //可以获得当前的windowEnd
            Long currentKey = ctx.getCurrentKey();
            mapState.put(value.getUrl(),value);

            //注册响应的计时器 窗口结束时间之后开始计算
            ctx.timerService().registerEventTimeTimer(currentKey + 1L);
            //定时定时器,当迟到数据处理完毕时,清空状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+60*1000L);
        }

        //定时器响起
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (timestamp == ctx.getCurrentKey() + 60000L) {
                //清空状态
                mapState.clear();
                //返回
                return;
            }

            //取出状态中的数据
            Iterator<Map.Entry<String, UrlCount>> iterator = mapState.entries().iterator();
            ArrayList<Map.Entry<String, UrlCount>> entries = Lists.newArrayList(iterator);

            //排序
            entries.sort(new Comparator<Map.Entry<String, UrlCount>>() {
                @Override
                public int compare(Map.Entry<String, UrlCount> o1, Map.Entry<String, UrlCount> o2) {
                    return (int)(o1.getValue().getCount() - o2.getValue().getCount());

                }
            });

            //准备输出数据
            StringBuilder sb = new StringBuilder();
            sb.append("===========").append(new Timestamp(timestamp - 1)).append("===========").append("\n");
            //遍历数据取出TopN

            for (int i = 0; i < Math.min(topSize, entries.size()); i++) {

                //取出单条数据
                UrlCount urlCount = entries.get(i).getValue();
                sb.append("Top ").append(i + 1);
                sb.append(" URL:").append(urlCount.getUrl());
                sb.append(" Count:").append(urlCount.getCount());
                sb.append("\n");
            }
            //休息
            Thread.sleep(100);
            //输出数据
            out.collect(sb.toString());
        }
    }
}
