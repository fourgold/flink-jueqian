package com.ecust.sink;

import com.ecust.util.KafkaTool;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author Jinxin Li
 * @create 2021-01-11 11:04
 */
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 获取kafka数据源
        KafkaTool test = new KafkaTool("test");
        FlinkKafkaConsumer011<String> kafkaData = test.getKafkaData();


        //0x2 处理kafka数据
        DataStreamSource<String> source = env.addSource(kafkaData);
        SingleOutputStreamOperator<String> words = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        });

        //0x3 将数据输出到redis
        words.print();

            //3.1 创建配置
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setPort(6379).setHost("hadoop102").build();
            //3.2 添加输出源
        words.addSink(new RedisSink<>(jedisPoolConfig, new RedisMapper<String>() {

            //3.2 指定写入的数据类型,如果是hash,需要额外输入指定key
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"sensor");
            }

            //3.3 指定Redis的key值,如果是HASH,指定的则是field
            @Override
            public String getKeyFromData(String data) {
                String[] words = data.split(",");
                return words[0];
            }

            //3.4 指定redis的元素值
            @Override
            public String getValueFromData(String data) {
                String[] words = data.split(",");
                return words[2];
            }
        }));

        //0x4 执行
        env.execute();
    }
}
