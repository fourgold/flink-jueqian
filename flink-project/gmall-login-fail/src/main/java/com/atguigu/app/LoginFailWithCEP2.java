package com.atguigu.app;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class LoginFailWithCEP2 {

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


        //定义模式序列
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        }).times(3).within(Time.seconds(5)).consecutive();

        //4.使用CEP处理数据
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);
        //5.继续处理数据
        patternStream.select(new MyPattern()).print();

        //6.执行
        env.execute();

    }

    public static class MyPattern implements PatternSelectFunction<LoginEvent,String> {
        @Override
        public String select(Map<String, List<LoginEvent>> map) throws Exception {
//            LoginEvent start = map.get("start").get(0);
//            LoginEvent end = map.get("end").get(0);
            LoginEvent start = map.get("start").get(0);
            LoginEvent end = map.get("start").get(map.get("start").size()-1);
            return "user:"+start.getUserId()+" time:"+start.getTimestamp()+"-"+end.getTimestamp()+"连续三次登陆失败";
        }
    }
}
