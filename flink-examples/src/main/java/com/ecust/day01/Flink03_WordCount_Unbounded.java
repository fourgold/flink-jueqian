package day01;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 参数优先级
 * 1.代码单独设置并行度
 * 2.代码全局:env.setParallelism(1);
 * 3.提交任务时参数:命令行(界面)
 * 4.集群配置文件
 */
public class Flink03_WordCount_Unbounded {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        env.setParallelism(4);

        //2.读取端口数据
        //ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //String host = parameterTool.get("host");
        //int port = parameterTool.getInt("port");

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //DataStreamSource<String> socketTextStream = env.socketTextStream(host, port);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());//.setParallelism(3);

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //5.计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);//.setParallelism(2);

        result.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return value.f1 == 1;
            }
        }).map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).print();

        //6.打印
        result.print("result:");

        //7.启动任务
        env.execute();
    }

}
