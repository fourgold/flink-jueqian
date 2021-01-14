package com.ecust.operators;

import com.ecust.beans.SensorReading;
import com.ecust.utils.KafkaTool;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Collections;

/**
 * @author Jinxin Li
 * @create 2021-01-09 16:45
 * todo connect 特点可以合并不同类型 connect->coMapFunction
 * connect:
 * 两个数据流被Connect之后，只是被放在了一个同一个流中，
 * 内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
 * CoMap,CoFlatMap:
 * 功能与map和flatMap一样，
 * 对ConnectedStreams中的每一个Stream分别进行map和flatMap处理。
 * map方法实现两次实现对两个流的统计
 */
public class Flink06_Operator_connectAndCoMap {
    public static void main(String[] args) throws Exception {
        //0x1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //0x2 从kafka读取数据流
        KafkaTool kafkaSource = new KafkaTool("testTopic1");
        FlinkKafkaConsumer011<String> kafkaDS = kafkaSource.getKafkaData();

            //2.1设定起始编译量 默认
        kafkaDS.setStartFromGroupOffsets();
        DataStreamSource<String> kafka = env.addSource(kafkaDS);

        //0x3 处理数据
        SingleOutputStreamOperator<SensorReading> string2Sensor = kafka.flatMap(new FlatMapFunction<String, SensorReading>() {
            @Override
            public void flatMap(String s, Collector<SensorReading> collector) throws Exception {
                String[] words = s.split(",");
                collector.collect(new SensorReading(words[0], Long.parseLong(words[1]), Double.parseDouble(words[2])));
            }
        });
            //3.1根据id进行分组
        KeyedStream<SensorReading, String> idStream = string2Sensor.keyBy(SensorReading::getId);

            //3.2分流,将高于30度的分流 变成二元组,将低于30温度流不合并
        SplitStream<SensorReading> splitStream = idStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemp() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
            //3.3分流之后将流使用select导出
        SingleOutputStreamOperator<Tuple2<String,Double>> high = splitStream.select("high").map(
                new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                        return new Tuple2<>(sensorReading.getId(),sensorReading.getTemp());
                    }
                });
        DataStream<SensorReading> low = splitStream.select("low");

        //0x4 合并 high流是一个tuple,低温流式一个sensorReading
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = high.connect(low);

            //4.1使用coMap将两个暂时放在一起的流进行合并  使用CoMapFunction
        SingleOutputStreamOperator<Object> result = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return stringDoubleTuple2;
            }
            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return sensorReading;
            }
        });

        //0x5 打印数据
        result.print();

        //0x6 执行
        env.execute();
    }
}
