package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取文本数据  hello atguigu flink
        DataSource<String> input = env.readTextFile("input");

        //3.压平操作  (hello,1) (atguigu,1)...
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = input.flatMap(new MyFlatMapFunc());

        //4.分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //5.计算WordCount
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //6.打印
        result.print();

    }

    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            //按照空格切分
            String[] words = value.split(" ");

            //遍历写出
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
