package practice;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.Set;

public class Flink03_DistinctWord_Redis {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        //4.去重
        wordToOne.filter(new MyRichFilterFunc()).print();

        env.execute();
    }

    public static class MyRichFilterFunc extends RichFilterFunction<Tuple2<String, Integer>> {

        private Jedis jedis;
        private String redisKey = "Words";

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop102", 6379);
        }

        @Override
        public boolean filter(Tuple2<String, Integer> value) throws Exception {

            //查询Redis是否存在该数据
            Set<String> set = jedis.smembers(redisKey);
            boolean contains = set.contains(value.f0);

            if (!contains) {
                jedis.sadd(redisKey, value.f0);
                return true;
            }

            return false;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

}
