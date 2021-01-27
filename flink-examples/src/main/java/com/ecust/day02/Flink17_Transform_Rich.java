package day02;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink17_Transform_Rich {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> sensorDS = env.socketTextStream("hadoop102", 9999);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = sensorDS.flatMap(new MyRichFlatMapFunc());

        //4.分组计算
        wordToOne.keyBy(0)
                .sum(1)
                .print();

        //5.执行任务
        env.execute();

    }

    public static class MyRichFlatMapFunc extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

        //生命周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //获取运行时上下文
            RuntimeContext context = getRuntimeContext();
            //状态编程
            ValueState<Object> valueState = context.getState(new ValueStateDescriptor<Object>("", Object.class));
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //切割
            String[] fields = value.split(" ");
            //遍历写出
            for (String field : fields) {
                out.collect(new Tuple2<>(field, 1));
            }
        }

        //生命周期方法
        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
