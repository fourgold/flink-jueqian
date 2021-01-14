package com.ecust.window;

import com.ecust.beans.SensorReading;
import com.ecust.source.Flink05_Source_UDFSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author JueQian
 * @create 01-12 16:38
 * 全量窗口函数
 * 先开窗,然后使用apply方法
 * 全量窗口函数有apply与process
 */
public class Flink04_TimeWindow_Tumbling_Apply {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x2 读取自定义流
        DataStreamSource<SensorReading> sensorSource = env.addSource(new Flink05_Source_UDFSource.MySource());


        //0x3 分组
        KeyedStream<SensorReading, String> one2Sensor = sensorSource.keyBy(SensorReading::getId);

        //0x4 开窗
        WindowedStream<SensorReading, String, TimeWindow> windowedStream = one2Sensor.timeWindow(Time.seconds(5));


        //0x5 全量聚合操作 需要调用apply方法
        SingleOutputStreamOperator<String> apply = windowedStream.apply(new MyWindowFunc());


        //0x5 打印数据
        apply.print();

        //0x6 执行
        env.execute();
    }

    /**
     * 全量窗口函数是一次性拿到所有数据,把所有数据都给你
     */
    private static class MyWindowFunc implements WindowFunction<SensorReading,String,String,TimeWindow>{
        @Override
        public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<String> out) throws Exception {
            //0x0 获取key
            String key = s;

            //0x1 获取当前数据条数 把迭代器拿出来,遍历一遍再使用
            int count = 0;
            Iterator<SensorReading> iterator = input.iterator();//迭代器要先拿出来,否则每次都是新的迭代器
            while (iterator.hasNext()){
                iterator.next();
                count+=1;
            }
            System.out.println("---------------------------------------------------");
            out.collect(s+":"+count);

        }
    }
}
