package com.atguigu.app;

import com.atguigu.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class LoginFailWithState1 {
    //如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据转换为JavaBean并提取时间戳生成Watermark
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new LoginEvent(Long.parseLong(fields[0]),
                                fields[1],
                                fields[2],
                                Long.parseLong(fields[3]));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.按照用户id分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        //4.使用ProcessFunction处理数据
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginFailProcessFunc1(2));

        //5.打印数据
        result.print();

        //6.执行
        env.execute();

    }

    public static class LoginFailProcessFunc1 extends KeyedProcessFunction<Long, LoginEvent, String> {
        private ListState<LoginEvent> listState;
        private ValueState<Long> state;
        private Integer interval;


        public LoginFailProcessFunc1(int interval) {
            this.interval=interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list-state", LoginEvent.class));
            state = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));


        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            Iterator<LoginEvent> iterator = listState.get().iterator();//获取失败名单状态
            Long ts = state.value();//获取失败数据状态

            if ("fail".equals(value.getEventType())){

                //将当前数据加入状态
                listState.add(value);
                //判断当前是否为第一条失败数据
                if(!iterator.hasNext()){
                    //注册后的interval的定时器
                    long curTs = (value.getTimestamp() + interval) * 1000L;
                    //定时
                    ctx.timerService().registerEventTimeTimer(curTs);
                    //更新时间状态
                    state.update(curTs);}
                }else {
                    if (ts!=null){
                        //说明定时器已经注册过
                        ctx.timerService().deleteEventTimeTimer(ts);
                    }

                    //清空状态
                    listState.clear();
                    state.clear();
//                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("定时器响了");
            //将迭代器转换为数据
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(listState.get().iterator());
            System.out.println(loginEvents.size());
            if (loginEvents.size() >= interval){
                System.out.println("报警了");
                out.collect(ctx.getCurrentKey()+"开始报警了...次数"+loginEvents.size());
            }
            state.clear();
            listState.clear();
        }
    }
}
