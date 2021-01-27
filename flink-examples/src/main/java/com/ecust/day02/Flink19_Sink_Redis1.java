package day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class Flink19_Sink_Redis1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //3.将数据写入Redis
        FlinkJedisPoolConfig.Builder builder = new FlinkJedisPoolConfig.Builder();
        FlinkJedisPoolConfig jedisPoolConfig = builder.setHost("hadoop102")
                .setPort(6379)
                .build();

        source.addSink(new RedisSink<String>(jedisPoolConfig,new MyRedisMapper1()));

        //4.执行任务
        env.execute();
    }


    private static class MyRedisMapper1 implements RedisMapper<String> {

        //指定写入数据类型,如果使用的是Hash或者ZSet,需要额外指定外层Key
        @Override
        public RedisCommandDescription getCommandDescription() {
            // todo 枚举类
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }

        //指定Redis中Key值(如果是Hash,指定的则是Field)
        @Override
        public String getKeyFromData(String s) {
            String[] word = s.split(" ");
            return word[0];
        }
        //指定Redis中Value值
        @Override
        public String getValueFromData(String s) {
            String[] word = s.split(" ");
            return word[1];
        }
    }
}
