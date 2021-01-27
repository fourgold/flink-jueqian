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

import java.util.Iterator;

//滚动时间窗口,计算最近10秒数据的WordCount
public class Flink08_Window_TimeTumbling_Apply1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境 压平返回tuple<0,1> 并且进行分组
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //2.开一个滚动窗口,将10秒内的数据进行进行统计
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(10));

        //3.聚合操作
        //我们可以使用sum方法,直接对数据进行统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);
        sum.print();

        //5.执行
        env.execute();

    }
}
