package day04;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/*
需求:使用侧输出流,使用ProcessAPI进行分流
 */
public class Flink10_ProcessAPI_SideOutPut2 {

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

        //4.使用ProcessAPI
        SingleOutputStreamOperator<String> lowDS = keyBy.process(new KeyedProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                if (Long.parseLong(value) > 30) {
                    out.collect(value);
                } else {
                    ctx.output(new OutputTag<String>("lowDS"){}, value);
                }
            }
        });

        lowDS.print("高流");

        DataStream<String> lowDS1 = lowDS.getSideOutput(new OutputTag<String>("lowDS"){});

        lowDS1.print("低硫");

        //5.执行
        env.execute();
    }
}
