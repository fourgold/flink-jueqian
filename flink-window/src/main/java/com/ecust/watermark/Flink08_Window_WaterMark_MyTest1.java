package com.ecust.watermark;

import com.ecust.beans.SensorReading;
import com.ecust.source.Flink05_Source_UDFSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * @author JueQian
 * @create 01-14 15:17
 * 这个程序主要根据源码重写waterMark方法
 * 在上个程序中,主要测试了watermark的生成
 * 但是使用的是端口数据
 * 端口数据发送较慢,一般一个数据后面会生成多个waterMark,不符合正常使用
 * 使用自定义数据源
 *
 * 参数:watermark间隔:2s
 * 滚动窗口:5s
 * 无序度:2s
 * 事件产生速率:0.3s
 */
public class Flink08_Window_WaterMark_MyTest1 {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境 并配置一些环境参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(2000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //0x1 获取自定义数据源
        DataStreamSource<SensorReading> sensor = env.addSource(new Flink05_Source_UDFSource.MySource());
        SingleOutputStreamOperator<SensorReading> singleOutputStreamOperator = sensor.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SensorReading>() {
            //当前流时间
            long currentMaxTimestamp = 0L;
            //当前流waterMark
            long lastEmittedWatermark = -9223372036854775808L;
            //当前流无序度
            long maxOutOfOrderness = 2000L;

            @Override
            public Watermark getCurrentWatermark() {
                long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
                //保证自增
                if (potentialWM >= this.lastEmittedWatermark) {
                    lastEmittedWatermark = potentialWM;
                }
                System.out.println("当前WaterMark为:" + lastEmittedWatermark);
                return new Watermark(lastEmittedWatermark);
            }

            @Override
            public long extractTimestamp(SensorReading sensorReading, long l) {
                Long timestamp = sensorReading.getTs();
                //如果事件的时间比较小,则不会更新时间戳
                currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
                System.out.println("key:" + sensorReading.getId() + ",EventTime:" + timestamp + ",水位线:" + lastEmittedWatermark);
                return timestamp;
            }
        });

        singleOutputStreamOperator.keyBy(SensorReading::getId).timeWindow(Time.seconds(5)).maxBy("temp").print("输出结果>>>>>>>>>>>>>");

        env.execute("test");
    }
}
