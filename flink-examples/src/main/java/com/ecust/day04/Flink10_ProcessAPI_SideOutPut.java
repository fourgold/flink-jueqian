package day04;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink10_ProcessAPI_SideOutPut {

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
                .process(new MySideOutPutProcessFunc());

        //4.打印
        result.print("high");
        //提取侧输出流并打印
        result.getSideOutput(new OutputTag<Tuple2<String, Double>>("sidePutPut") {
        }).print("low");

        //5.执行
        env.execute();
    }

    //使用ProcessAPI实现高低温分流操作
    public static class MySideOutPutProcessFunc extends KeyedProcessFunction<String, String, String> {

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            //切割数据,获取其中的温度值
            String[] fields = value.split(",");
            double temp = Double.parseDouble(fields[2]);

            //判断温度决定输出数据
            if (temp > 30.0) {

                //高温数据,输出到主流
                out.collect(value);

            } else {
                //低温数据,输出到侧输出流
                ctx.output(new OutputTag<Tuple2<String, Double>>("sidePutPut") {
                }, new Tuple2<>(fields[0], temp));
            }
        }
    }

}
