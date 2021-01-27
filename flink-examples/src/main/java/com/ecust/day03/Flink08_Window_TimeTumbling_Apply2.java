package day03;

import day01.Flink01_WordCount_Batch;
import org.apache.commons.compress.utils.Lists;
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

import java.lang.management.PlatformLoggingMXBean;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;

//滚动时间窗口,计算最近10秒数据的WordCount
public class Flink08_Window_TimeTumbling_Apply2 {


    public static void main(String[] args) throws Exception {

        //1.获取执行环境 压平返回tuple<0,1> 并且进行分组
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);
        System.out.println("keyby结束");

        //2.开一个滚动窗口,将10秒内的数据进行进行统计
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(10));

        //3.聚合操作
        //我们可以使用sum方法,直接对数据进行统计
        windowedStream.apply(new MyWindowFunc()).print();

        //5.执行
        env.execute();

    }

    public static class MyWindowFunc implements WindowFunction<Tuple2<String, Integer>,Tuple2<String, Integer>,Tuple, TimeWindow> {
        private int all;
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("apply方法");
            long start = window.getStart();
            long end = window.getEnd();
            int count = 0;
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();
            //input,是把所有的东西累加放在一个迭代器里,然后一起
            while (iterator.hasNext()){
                count = count + iterator.next().f1;
                all = all+input.iterator().next().f1;
            }
            Iterator<Tuple2<String, Integer>> iterator1 = input.iterator();
            ArrayList<Tuple2<String, Integer>> tuple2s = Lists.newArrayList(iterator1);
            System.out.println("转换成array:"+tuple2s.toString());

            System.out.println("all:"+all);
            System.out.println("=================="+start+"================"+end+"==============");
            out.collect(new Tuple2<>(tuple.getField(0),count));
        }
    }

    /**
     * window方程式,是将每一个窗口的每一个key放到单独的一个集合iterable里,
     */
    public static class MyWindowFunc2 implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {


        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd,HH-mm-ss");
            //获取key值
            String key = tuple.getField(0);
            long start = window.getStart();
            long end = window.getEnd();

            //获取当前数据的条数
            int count = 0;
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();
            while (iterator.hasNext()) {
                count += iterator.next().f1;
            }
            Iterator<Tuple2<String, Integer>> iterator1 = input.iterator();

            System.out.println("=================="+sdf.format(start)+"================"+sdf.format(end)+"==============");
            //输出数据
            out.collect(new Tuple2<>(key, count));
        }
    }
}
