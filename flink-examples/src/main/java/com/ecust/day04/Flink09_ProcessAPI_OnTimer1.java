package day04;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink09_ProcessAPI_OnTimer1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //3.使用ProcessAPI测试定时器功能
        KeyedStream<String, String> keyBy = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });
        SingleOutputStreamOperator<String> result = keyBy.process(new MyOnTimerProcessFunc1());

        //4.打印
        result.print();

        //5.执行
        env.execute();
    }

    private static class MyOnTimerProcessFunc1 extends KeyedProcessFunction<String,String,String>{
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }

            //注册两秒后定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+2000L);
        }
        //设定定时器触发
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("定时器触发");
//            ctx.timerService().deleteProcessingTimeTimer();
        }
    }
}
