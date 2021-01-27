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
 * @create 01-20 18:55
 * 测试java8新特性
 */
public class Flink03_Require_TimeOut_Advance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

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

        Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getEvent().equals("add");
            }
        }).followedBy("middle").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getEvent().equals("order");
            }
        }).notFollowedBy("end").where(new SimpleCondition<Event>() {
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
    }
}
