package com.ecust.sink;

import com.ecust.beans.SensorReading;
import com.ecust.source.Flink05_Source_UDFSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Jinxin Li
 * @create 01-12 0:39
 */
public class Flink04_Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new Flink05_Source_UDFSource.MySource());

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));

        ElasticsearchSink.Builder<SensorReading> builder = new ElasticsearchSink.Builder<>(httpHosts, new MyEsSinkFunction());
        builder.setBulkFlushInterval(1);

        source.addSink( builder.build());

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
                    .type("_doc")
                    .source(dataSource);

            indexer.add(indexRequest);
        }
    }
}
