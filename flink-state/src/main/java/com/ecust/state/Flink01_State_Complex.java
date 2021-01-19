package com.ecust.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JueQian
 * @create 01-16 17:23
 * 需求:检测传感器的温度值，如果连续的两个温度差值超过10度，就输出报警。
 *
 * 注意富函数,必须输出
 * process可能不用输出,因为有collector
 *
 * 富函数实现状态编程
 */
public class Flink01_State_Complex {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //0x1 从端口获取数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //0x2 从将数据进行分流
        KeyedStream<String, String> keyedStream = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                String[] fields = s.split(",");
                System.out.println("key"+fields[0]);
                return fields[0];
            }
        });

        //0x3 讲数据变为使用map处理
        SingleOutputStreamOperator<String> map = keyedStream.map(new MyRichMapFunc(10.0));

        //0x4 打印
        map.print();

        //0x5 执行
        env.execute();
    }

    public static class MyRichMapFunc extends RichMapFunction<String,String> {

        private Double interval;
        private ValueState<Double> temp;

        public MyRichMapFunc() {
        }

        public MyRichMapFunc(Double interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            temp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp", Double.class));
        }

        @Override
        public String map(String s) throws Exception {
            //获取温度数据
            Double lastTemp = temp.value();
            System.out.println("lastTemp:"+lastTemp);

            //获取时间数据
            String[] fields = s.split(",");
            String field = fields[2];
            double v = Double.parseDouble(field);

            if (lastTemp==null){
                System.out.println("第一个数据");
            }else if (v-lastTemp > interval){
                System.out.println("温度超过10度");
            }else {

            }
            temp.update(v);
            return s;
        }
    }
}
