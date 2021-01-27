package day04;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink09_ProcessAPI_OnTimer {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.使用ProcessAPI测试定时器功能
        SingleOutputStreamOperator<String> result = socketTextStream
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                })
                .process(new MyOnTimerProcessFunc());

        //4.打印
        result.print();

        //5.执行
        env.execute();
    }

    public static class MyOnTimerProcessFunc extends KeyedProcessFunction<String, String, String> {


        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            //输出数据
            out.collect(value);

            //注册2秒后的定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
        }

        //定时器触发的任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("定时器触发！！！");
        }

    }

}
