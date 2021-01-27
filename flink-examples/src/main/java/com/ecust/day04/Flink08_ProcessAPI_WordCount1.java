package day04;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Jinxin Li
 * @create 2020-12-14 10:23
 */
public class Flink08_ProcessAPI_WordCount1 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //压平
        SingleOutputStreamOperator<String> wordDS = source.process(new MyFlatMapProcess());

        //将每个单词转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = wordDS.process(new MyMapProcess());

        //5.分组聚合,keyby可以接受多个单词
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = tupleDS.keyBy(0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.process(new MySumProcess());

        result.print();

        env.execute();


    }

    private static class MyFlatMapProcess extends ProcessFunction<String,String>{
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            //切割
            String[] split = value.split(" ");

            for (String s : split) {
                out.collect(s);
            }

        }
    }

    private static class MyMapProcess extends ProcessFunction<String, Tuple2<String,Integer> >{

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value,1));
        }
    }
    //考虑到通用性,传tuple
    private static class MySumProcess extends KeyedProcessFunction<Tuple,Tuple2<String,Integer>,Tuple2<String,Integer>> {

        private Integer count = 0;

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value.f0, ++count));
        }
    }
}
