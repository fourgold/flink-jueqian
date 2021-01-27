package com.atguigu.app;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UvCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;

public class UvCountApp2_1 {

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
        AllWindowedStream<UserBehavior, TimeWindow> allWindowedStream = userBehaviorDS.timeWindowAll(Time.hours(1));

        //4.自定义触发器,实现来一条数据则计算一条数据,使用ProcessFunction去重数据
        SingleOutputStreamOperator<UvCount> result = allWindowedStream.trigger(new Trigger<UserBehavior, TimeWindow>() {
            @Override
            public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.FIRE_AND_PURGE;
            }

            @Override
            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            }
        }).process(new MyProcess());

        //5.打印数据
        result.print();

        //6.执行
        env.execute();

    }
    //一个小时处理一次
    private static class MyProcess extends ProcessAllWindowFunction<UserBehavior, UvCount, TimeWindow> {
        //获取Redis连接
        private Jedis jedisClient;
        //定义存储每个小时用户访问量的hashkey
        private String hourUvRedisKey;
        //声明布隆过滤器
        private MyBooleanFilter myFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedisClient = new Jedis("hadoop102", 6379);
            myFilter = new MyBooleanFilter(1 << 30);

            //设计存储统计量的key
            hourUvRedisKey = "hourUV";

        }
        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<UvCount> out) throws Exception {

            //获取窗口结束时间
            Timestamp windowEnd = new Timestamp(context.window().getEnd());

            //注意迭代器里只有一个元素 ,把userId转换为位图里面的一位
            Long offset = myFilter.getOffset(elements.iterator().next().getUserId().toString());

            //获取布隆过滤器的key
            String booleanKey = "boolean:" + windowEnd;
            Boolean bit = jedisClient.getbit(booleanKey, offset);

            //把窗口结束时间当作一个小时的key
            String field = windowEnd.toString();

            //判断 去重
            if (!bit){
                jedisClient.setbit(booleanKey,offset,true);

                jedisClient.hincrBy(hourUvRedisKey,field,1L);
            }
            out.collect(new UvCount(field,Long.parseLong(jedisClient.hget(hourUvRedisKey,field))));
        }
        @Override
        public void close() throws Exception {
            jedisClient.close();
        }

    }
    public static class MyBooleanFilter {
        //给布隆过滤器定义一个容量
        //需求传入2的整次方幂l
        private long capacity;

        public MyBooleanFilter(long capacity) {
            this.capacity = capacity;
        }

        //获得位置信息
        public Long getOffset(String value){
            long result = 0;

            for (char c : value.toCharArray()) {
                result = result*31 + c;
            }

            //位运算取模,充分方法混乱度
            return result & (capacity-1);
        }

    }
}
