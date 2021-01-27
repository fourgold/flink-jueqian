package day04;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink07_ProcessAPI_Test {

    public static void main(String[] args) {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("", 9999);

        //3.使用ProcessAPI
        SingleOutputStreamOperator<String> process = socketTextStream.process(new MyProcessFunc());

        //4.获取侧输出流数据
        DataStream<String> sideOutput = process.getSideOutput(new OutputTag<String>("outPutTag") {
        });

    }

    public static class MyProcessFunc extends ProcessFunction<String, String> {

        //声明周期方法
        @Override
        public void open(Configuration parameters) throws Exception {

            //获取运行时上下文,做状态编程
            RuntimeContext runtimeContext = getRuntimeContext();
//            runtimeContext.getState();
//            runtimeContext.getListState();
//            runtimeContext.getMapState();
//            runtimeContext.getReducingState();
//            runtimeContext.getAggregatingState();
        }

        //处理进入系统的每一条数据
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            //输出数据
            out.collect("");

            //获取处理时间相关数据并注册和删除定时器
            ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(1L);
            ctx.timerService().deleteProcessingTimeTimer(1L);

            //获取事件时间相关数据并注册和删除定时器
            ctx.timerService().currentWatermark();
            ctx.timerService().registerEventTimeTimer(1L);
            ctx.timerService().deleteEventTimeTimer(1L);

            //侧输出流
            ctx.output(new OutputTag<String>("outPutTag") {
            }, "");

        }

        //定时器触发任务执行
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        }

        //声明周期方法
        @Override
        public void close() throws Exception {

        }
    }

}
