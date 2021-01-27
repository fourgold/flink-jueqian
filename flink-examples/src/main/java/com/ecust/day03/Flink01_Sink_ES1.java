package day03;

import bean.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.hadoop.shaded.org.apache.commons.httpclient.HttpHost;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Jinxin Li
 * @create 2020-12-12 10:58
 */
public class Flink01_Sink_ES1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));

//        source.addSink( new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());


        env.execute();
    }
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {
        @Override
        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {

            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", element.getId());
            dataSource.put("ts", element.getTs().toString());
            dataSource.put("temp", element.getTemp().toString());

            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingData")
                    .source(dataSource);

            indexer.add(indexRequest);
        }
    }

}
