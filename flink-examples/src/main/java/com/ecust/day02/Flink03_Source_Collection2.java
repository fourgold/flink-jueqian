package day02;

import bean.SensorReading;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.ArrayIterator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Flink03_Source_Collection2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<SensorReading> list = Arrays.asList(new SensorReading("a", System.currentTimeMillis(), 28.0));
        DataStreamSource<SensorReading> source = env.fromCollection(list);

        source.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
