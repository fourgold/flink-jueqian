package com.ecust.source;

import com.ecust.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author Jinxin Li
 * @create 2021-01-08 21:07
 */
public class Flink05_Source_UDFSource {
    public static void main(String[] args) throws Exception {
        //0x0 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 获取执行环境
        DataStreamSource<SensorReading> sensorDS = env.addSource(new MySource());

        //0x2 打印环境
        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading sensorReading) throws Exception {
                return sensorReading.getId() + "," + sensorReading.getTs() + "," + sensorReading.getTemp();
            }
        });
        map.print();

        //0x3 启动环境
        env.execute();
    }

    //0x0 定义自定义数据源
    public static class MySource implements SourceFunction<SensorReading>{

        //定义标记控制数据的运行
        private boolean running = true;

        //定义一个随机数据
        private Random random = new Random();


        //定义基准温度数组
        private Map<String,SensorReading> map = new HashMap<String,SensorReading>();

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            //0x0 造数据 给传感器赋值
            for (int i = 0; i < 10; i++) {
                SensorReading sensorReading = map.put("sensor_" + i, new SensorReading("sensor_" + (i+1), System.currentTimeMillis()-10000000L, 60D+random.nextGaussian() * 20));
            }
            while (running){
                //0x1 发送数据
                for (String s : map.keySet()) {
                    //设定温度
                    double v = map.get(s).getTemp() + random.nextGaussian() * 4;
                    double v1 = (double) Math.round(v * 100) / 100;
                    //每次温度都要增大
                    map.get(s).setTemp(v1);

                    //设定时间每次都要增大
                    long ts = map.get(s).getTs() + random.nextInt(2000);
                    SensorReading sensorReading = map.get(s);
                    sensorReading.setTs(ts);
//                    System.out.println(i);
//                    System.out.println(map.get(s).getTs());

                    //写出数据
                    sourceContext.collect(sensorReading);

                    //停顿一下
                    Thread.sleep(10);
                }
            }
        }

        @Override
        public void cancel() {
            running=false;

        }
    }
}
