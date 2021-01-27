package day03;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

//滚动时间窗口,计算最近10秒数据的WordCount
public class Flink08_Window_TimeTumbling_Apply {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //5.开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(10));

        //6.聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.apply(new MyWindowFunc());

        //7.打印
        result.print();

        //8.执行
        env.execute();

    }

    public static class MyWindowFunc implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {

            //获取key值
            String key = tuple.getField(0);

            //获取当前数据的条数
            int count = 0;
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();
            while (iterator.hasNext()) {
                count += iterator.next().f1;
            }

            //输出数据
            out.collect(new Tuple2<>(key, count));
        }
    }
}
