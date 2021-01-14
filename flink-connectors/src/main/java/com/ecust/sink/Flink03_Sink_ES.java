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
 * @create 2021-01-11 20:32
 */
public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //0x1 获取自定义数据源
        DataStreamSource<SensorReading> source = env.addSource(new Flink05_Source_UDFSource.MySource());

        //0x3 将数据写入ES
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));

        ElasticsearchSink.Builder<SensorReading> builder = new ElasticsearchSink.Builder<>(httpHosts, new MyESSinkFunc());
        //设置刷写条数
        builder.setBulkFlushInterval(1);

        source.addSink(builder.build());

        //0x4 执行
        env.execute();
    }

    //实现sinkfunction
    public static class MyESSinkFunc implements ElasticsearchSinkFunction<SensorReading>{
        //RequestIndexer 往es切割
        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            //获取温度字段
            Double temp = sensorReading.getTemp();
            Long ts = sensorReading.getTs();
            String id = sensorReading.getId();

            //创建Map用于存放数据k-v数据
            HashMap<String, String> source = new HashMap<>();
            source.put("id",id);
            source.put("ts",ts.toString());
            source.put("temp",temp.toString());

            System.out.println(source.toString());

            //构建IndexRequest对象
            IndexRequest source1 = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
                    .source(source);
            requestIndexer.add(source1);
        }
    }

}
