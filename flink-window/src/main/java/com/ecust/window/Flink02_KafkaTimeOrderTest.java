package com.ecust.window;

import com.ecust.beans.SensorReading;
import com.ecust.source.Flink05_Source_UDFSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author JueQian
 * @create 01-12 14:15
 * 测试kafka是否能够产生乱序数据
 */
public class Flink02_KafkaTimeOrderTest {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
                //时间格式化
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //0x1 从执行环境中获取
        DataStreamSource<SensorReading> sensorSource = env.addSource(new Flink05_Source_UDFSource.MySource());

        SingleOutputStreamOperator<String> map = sensorSource.map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading sensorReading) throws Exception {
                return sensorReading.getId() + ":" + simpleDateFormat.format(new Date(sensorReading.getTs()));
            }
        });

        map.print();

        //0x2 使用将数据写入Kafka
        map.addSink(new FlinkKafkaProducer011<String>("hadoop102:9092", "timeTest", new SimpleStringSchema()));


        //0x3 执行
        env.execute();
    }
}
