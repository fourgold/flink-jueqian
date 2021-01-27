package practice;

import day01.Flink01_WordCount_Batch;
import day02.Flink04_Source_File2;
import day04.Flink08_ProcessAPI_WordCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.print.DocFlavor;
import java.util.List;
import java.util.Properties;

/**
 * @author Jinxin Li
 * @create 2020-12-17 15:21
 * 使用FlinkSQL实现从Kafka读取数据计算WordCount并将数据写入ES中
 */
public class Flink06_SQL_Kakfa_WordCount_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"BigDataTest");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));

        //扁平
        SingleOutputStreamOperator<String> process = source.process(new MyFlatProcess());

        //建表
        tableEnv.createTemporaryView("wordCount",process,"word,count");
        Table table = tableEnv.fromDataStream(source);
        Table word = table.groupBy("word").select("word,sum(count)");

        tableEnv.toRetractStream(word,Row.class).print();


        //elasticSearchProperty
        Elasticsearch elasticsearchProperty = new Elasticsearch()
                .version("6")
                .host("hadoop102", 9200, "http")
                .index("wordCount")
                .documentType("_doc")
                .bulkFlushMaxActions(1);

        Schema schema = new Schema().field("word", DataTypes.STRING()).field("count", DataTypes.INT());

        tableEnv.connect(elasticsearchProperty)
                .withFormat(new Json())
                .withSchema(schema)
                .inUpsertMode()
                .createTemporaryTable("esWords");



        env.execute();
    }


    private static class MyFlatProcess extends ProcessFunction<String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            String[] fields = value.split(",");
            for (String field : fields) {
                out.collect("field,1");
            }
        }
    }
}
