package com.ecust.processapi;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Int;

/**
 * @author JueQian
 * @create 01-16 15:29
 * 按照温度分流
 */
public class Flink03_ProcessAPI_SideOutput {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 从端口获取数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        KeyedStream<String, String> keyedStream = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                String[] fields = s.split(",");
                return fields[0];
            }
        });

        //0x2 定义辨别API
        SingleOutputStreamOperator<String> result = keyedStream.process(new MyKeyedProcess());


        //0x3 打印
        result.print();
        result.getSideOutput(new OutputTag<String>("side"){}).print("side");


        //0x4 执行
        env.execute();

    }
    public static class MyKeyedProcess extends KeyedProcessFunction<String,String,String> {

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            String[] fields = value.split(",");

            //如果温度大于30度,则到主流
            if (Long.parseLong(fields[2]) > 30L){
                out.collect(value);
            }else{
                //否则去次流
                ctx.output(new OutputTag<String>("side"){},value);
            }

        }
    }
}
