package com.ecust.example;

import com.ecust.beans.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author JueQian
 * @create 01-20 15:14
 */
public class Flink01_Require_TimeOut {
    public static void main(String[] args) throws Exception {
        //0x0 环境 端口数据 配置checkpoint 指定事件时间语义 使用事件时间 转换为样例类
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        /*
        没有hadoop依赖所以暂时不添加
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/flinkCk"));
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);*/

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<String> eventString = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(1L)) {
            @Override
            public long extractTimestamp(String s) {
                String[] fields = s.split(",");
                return Long.parseLong(fields[1]);
            }
        });

        SingleOutputStreamOperator<Event> mapStream = eventString.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {
                String[] fields = s.split(",");
                String id = fields[0];
                long ts = Long.parseLong(fields[1]);
                String event = fields[2];
                return new Event(id, ts, event);
            }
        });

        //0x1 设置匹配模式
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {

                return event.getEvent().equals("add");
            }
        }).followedBy("mid").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getEvent().equals("order");
            }
        }).followedBy("end").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getEvent().equals("pay");
            }
        }).within(Time.seconds(10));

        //0x2 将数据流pattern整合
        KeyedStream<Event, String> keyedStream = mapStream.keyBy(Event::getId);
        PatternStream<Event> patternStream = CEP.pattern(keyedStream, pattern);

        //0x3 处理数据流
        OutputTag<String> outputTag = new OutputTag<String>("timeOut") {};
        SingleOutputStreamOperator<String> mainStream = patternStream.flatSelect(outputTag, new PatternFlatTimeoutFunction<Event, String>() {
            //超时数据
            @Override
            public void timeout(Map<String, List<Event>> map, long timeoutTs, Collector<String> out) throws Exception {
                Stream<List<Event>> stream = map.values().stream();
                Stream<Event> eventStream = stream.flatMap(elems -> elems.stream());
                Stream<String> stringStream = eventStream.map(event -> event.toString());
                String collect = stringStream.collect(Collectors.joining("->"));
                out.collect(collect);
            }
        }, new PatternFlatSelectFunction<Event, String>() {
            //主流数据
            @Override
            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                System.out.println("------------主流--------------");
                collector.collect(map.values().stream().flatMap(elem -> elem.stream()).map(elem -> elem.toString()).collect(Collectors.joining("->")));
            }
        });

        //0x4 打印数据流
        DataStream<String> timeoutStream = mainStream.getSideOutput(outputTag);
        timeoutStream.print("未支付的数据");
        mainStream.print("主流数据");

        //0x5 执行计划
        env.execute();

        //add未支付的数据:6> Event(id=user1, ts=1547718210000, event=add)
        //未支付的数据:6> Event(id=user1, ts=1547718225000, event=add)->Event(id=user1, ts=1547718230000, event=order)
        //未支付的数据:1> Event(id=user2, ts=1547718235000, event=add)->Event(id=user2, ts=1547718240000, event=order)

        //这种做法的计算结果是会存在脏数据的，因为这个规则不仅匹配到了下单并且预付款后超时未被接单的订单（想要的结果），同样还匹配到了只有下单行为后超时未被接单的订单（脏数据，没有预付款）。
    }
}


/*
Pattern<Event, ?> start = Pattern.<Event>begin(
    Pattern.<Event>begin("start").where(...).followedBy("middle").where(...)
);i
 */