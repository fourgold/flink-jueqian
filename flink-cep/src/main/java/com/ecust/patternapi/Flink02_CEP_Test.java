package com.ecust.patternapi;


import com.ecust.beans.SensorAuto;
import com.ecust.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;


/**
 * @author JueQian
 * @create 01-20 9:44
 * 官网案例
 */

public class Flink02_CEP_Test {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //0x1 执行pattern
        Pattern<SensorReading, ?> pattern = Pattern.<SensorReading>begin("start").where(
                new SimpleCondition<SensorReading>() {
                    //从温度到42度开始
                    @Override
                    public boolean filter(SensorReading sensorReading) {
                        return sensorReading.getTemp() == 42;
                    }
                }//然后温度接下来又是42度不会跳变
                //todo 严格紧邻模式Flink02_CEP_Test
        ).next("middle").subtype(SensorAuto.class).where(
                new SimpleCondition<SensorAuto>() {
                    @Override
                    public boolean filter(SensorAuto subSensorReading) {
                        return subSensorReading.getTemp() >= 42;
                    }
                }
                //todo 非严格紧邻模式
        ).followedBy("end").where(
                new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading sensorReading) {
                        return sensorReading.getId().equals("end");
                    }
                }
        );

        //0x2 将数据流映射为样例类
        SingleOutputStreamOperator<SensorReading> operator = source.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                String id = fields[0];
                long ts = Long.parseLong(fields[1]);
                double temp = Double.parseDouble(fields[2]);
                return new SensorReading(id, ts, temp);
            }
        });

        //0x3 将流进行按key分组
        KeyedStream<SensorReading, String> keyedStream = operator.keyBy(SensorReading::getId);

        //0x4 将pattern作用于流
        PatternStream<SensorReading> patternStream = CEP.pattern(keyedStream, pattern);

        //0x5 将流选择出来
        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<SensorReading, String>() {
            @Override
            public String select(Map<String, List<SensorReading>> map) throws Exception {
                //注意这个数据格式Map<定义的模式名,符合模式名的事件列表>
                SensorReading start = map.get("start").get(0);
                SensorReading end = map.get("mid").get(0);
//                return start.get(0).getId()+"在"+start.get(0).getTs()+"-"+end.get(0).getTs()+"温度超过30度不下降,当前温度"+end.get(0).getTemp();
                return start+"-"+end;
            }
        });

        //0x6 打印
        select.print();

        //0x7 执行计划
        env.execute();
    }
}
