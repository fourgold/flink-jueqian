package day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink04_Source_File2 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> source = env.readTextFile("./input");

        //todo 转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapTuple = source.flatMap(new MyFlatMapFunc1());

        // todo 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = flatMapTuple.keyBy(0);
        keyBy.print("keyBy");

        // 统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);

        //3.打印
        sum.print("字符统计");

        //4.执行任务
        env.execute();

    }

    public static class MyFlatMapFunc1 implements FlatMapFunction<String, Tuple2<String,Integer>> {

        /**
         * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
         * it into zero, one, or more elements.
         *
         * @param value The input value.
         * @param out   The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] strings = value.split(" ");
            for (String word : strings) {
                Tuple2<String, Integer> word2One = new Tuple2<>(word, 1);
                out.collect(word2One);
            }
        }
    }
}
