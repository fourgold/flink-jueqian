package day04;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink08_ProcessAPI_WordCount {


    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.压平
        SingleOutputStreamOperator<String> wordDS = socketTextStream.process(new MyFlatMapProcessFunc());

        //4.将每个单词转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = wordDS.process(new MyMapProcessFunc());

        //5.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);

        //6.聚合数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.process(new MySumKeyedProcessFunc());

        //7.打印
        result.print();

        //8.执行
        env.execute();
    }

    public static class MyFlatMapProcessFunc extends ProcessFunction<String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //切割
            String[] words = value.split(",");
            //遍历输出
            for (String word : words) {
                out.collect(word);
            }
        }
    }

    public static class MyMapProcessFunc extends ProcessFunction<String, Tuple2<String, Integer>> {
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value, 1));
        }
    }

    public static class MySumKeyedProcessFunc extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private Integer count = 0;

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            count += 1;

            out.collect(new Tuple2<>(value.f0, count));
        }
    }
}