package com.atguigu.app;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 所以为了让用户更有紧迫感从而提高支付转化率，
 * 同时也为了防范订单支付环节的安全风险，
 * 电商网站往往会对订单状态进行监控，
 * 设置一个失效时间（比如15分钟），
 * 如果下单后一段时间仍未支付，订单就会被取消。
 *
 * //第三个字段是支付流水号
 */
public class OrderPayWithCepApp1 {
    public static void main(String[] args) throws Exception {
        //1.获得执行环境 并且设定时间语义
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<OrderEvent> source = env.readTextFile("./input/OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        //ID进行分组
        KeyedStream<OrderEvent, Long> keyedStream = source.keyBy(OrderEvent::getOrderId);

        //2.定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        //应用模式序列
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //提取事件,(匹配数据与超时数据)
        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("timeOutEvent"){}, new MyTimeOutFunc(), new MySelectFunc());

        result.print("正常支付");
        result.getSideOutput(new OutputTag<String>("timeOutEvent"){}).print("超时数据");
        env.execute();
    }
    public static class MySelectFunc implements PatternSelectFunction<OrderEvent,String>{

        @Override
        public String select(Map<String, List<OrderEvent>> map) throws Exception {
            OrderEvent start = map.get("start").get(0);
            OrderEvent follow = map.get("follow").get(0);
            return start.getOrderId()+":"+start.getEventTime()+"-"+follow.getEventTime()+" 成功支付.";
        }
    }

    public static class MyTimeOutFunc implements PatternTimeoutFunction<OrderEvent,String>{

        @Override
        public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            OrderEvent start = pattern.get("start").get(0);
            return start.getOrderId() + ":" + start.getEventTime()+" 订单支付超时";
        }
    }
}
