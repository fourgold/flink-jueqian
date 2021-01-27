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

/**
 * @author Jinxin Li
 * @create 2020-12-14 10:23
 */
public class Flink07_ProcessAPI_Test1 {
    public static void main(String[] args) {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9092);

        SingleOutputStreamOperator<String> process = source.process(new ProcessFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {

                //获取运行时上下文,做状态编程
                RuntimeContext runtimeContext = getRuntimeContext();
//                runtimeContext.getState();
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            //处理进入系统的每一天数据
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {


                //输出数据
                out.collect("输出");

                //获取时间
                ctx.timerService().currentProcessingTime();
                ctx.timerService().registerEventTimeTimer(1L);
                ctx.timerService().deleteProcessingTimeTimer(1L);

                //获取事件时间相关,获取事件时间相关数据,并且删除定时器
                ctx.timerService().currentWatermark();
                ctx.timerService().registerEventTimeTimer(1L);
                ctx.timerService().deleteEventTimeTimer(1L);

                //侧输出流???????????????????
                ctx.output(new OutputTag<String>("outputtag"),"hello");

            }

            //定时器触发任务执行
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {


            }
        });


        //获取侧输出流数据
        DataStream<String> outputtag = process.getSideOutput(new OutputTag<String>("outputtag"));

    }
}
