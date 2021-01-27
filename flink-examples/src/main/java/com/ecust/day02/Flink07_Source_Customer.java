package day02;

import bean.SensorReading;
import jdk.nashorn.internal.ir.Flags;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.TunnelRefusedException;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Flink07_Source_Customer {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从自定义数据源读取数据
        DataStreamSource<SensorReading> sensorReadingDS = env.addSource(new MySource());

        //3.打印
        sensorReadingDS.print();

        //4.启动
        env.execute();

    }

    private static class MySource implements SourceFunction<SensorReading> {

        private boolean flag = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            for (int i = 0; i < 5; i++) {
                String id = "sensor-" + ( i + 1);
                SensorReading reading = new SensorReading(id, System.currentTimeMillis(), 60D);
                ctx.collect(reading);
            }
        }
        @Override
        public void cancel() {
            flag = false;
        }
    }
}
