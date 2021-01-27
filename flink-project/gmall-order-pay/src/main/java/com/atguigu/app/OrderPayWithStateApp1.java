package com.atguigu.app;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.awt.*;

public class OrderPayWithStateApp1 {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean并提取时间戳生成Watermark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
//        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        //3.按照OrderID进行分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        SingleOutputStreamOperator<String> result = keyedStream.process(new OrderTimeOutProcess());

        result.getSideOutput(new OutputTag<String>("NoOrder"){}).print("只有支付数据:");
        result.getSideOutput(new OutputTag<String>("timeOutNoPay"){}).print("只有订单数据:");
        result.print("下单并支付:");

        env.execute();


    }

    private static class OrderTimeOutProcess extends KeyedProcessFunction<Long, OrderEvent,String>{
        //用于存储订单信息的状态
        private ValueState<OrderEvent> createState;

        //用于存储时间的状态
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //用于存储下单状态,从下单开始计时
            createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order", OrderEvent.class));
            //用于存储时间
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts", Long.class));

        }


        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            //获取状态数据

            //判断当前数据类型
            if ("create".equals(value.getEventType())){
                //如果是创建数据,计时器
                Long timing = (value.getEventTime()+15*60)*1000L;
                ctx.timerService().registerEventTimeTimer(timing);

                //更新状态
                createState.update(value);
                tsState.update(timing);
            } else if("pay".equals(value.getEventType())){

                OrderEvent lastEvent = createState.value();
                Long lastTime = tsState.value();

                if (lastEvent == null){
                    ctx.output(new OutputTag<String>("NoOrder"){},value.getOrderId()+"只有支付数据,没有订单数据");
                }else{
                    //15分钟以内支付
                    out.collect(value.getOrderId()+" "+lastEvent.getEventTime()+"-"+value.getEventTime()+" 被成功支付");
                    ctx.timerService().deleteEventTimeTimer(lastTime);
                    createState.clear();
                    tsState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("aaaaaaaaaaaaaaaaaa");

           //取出状态数据
            OrderEvent orderEvent = createState.value();
            ctx.output(new OutputTag<String>("timeOutNoPay"){},orderEvent.getOrderId()+"订单失效了");

            createState.clear();
            tsState.clear();

        }
    }
}
