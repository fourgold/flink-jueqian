package day04;

import bean.SensorReading;
import day02.Flink04_Source_File2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink12_State_TempDiff3 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //2.读取端口数据创建流,转换为JavaBean,同时按照sensor_id分组
        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunc7());
        KeyedStream<SensorReading, String> keyBy = map.keyBy(SensorReading::getId);

        //3.使用RichFunction实现状态编程,如果同一个传感器连续两次温度差值超过10度,则输出报警信息
        SingleOutputStreamOperator<String> map1 = keyBy.map(new MyRichFlatFunc3(10D));

        //4.打印数据
        map1.print("状态");

        //5.执行
        env.execute();


    }

    //转换成样例类
    private static class MyMapFunc7 implements MapFunction<String,SensorReading> {
        @Override
        public SensorReading map(String value) throws Exception {
            String[] str = value.split(",");
            return new SensorReading(str[0],Long.parseLong(str[1]),Double.parseDouble(str[2]));
        }
    }


    private static class MyRichFlatFunc3 extends RichMapFunction<SensorReading,String>{
        private ValueState<Double> state;
        private Double max_diff;

        public MyRichFlatFunc3(Double max_diff) {
            this.max_diff = max_diff;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<Double>("maxDiff", Double.class));
        }

        @Override
        public String map(SensorReading value) throws Exception {
            Double lastTemp = state.value();
            Double currentTemp = value.getTemp();
            state.update(currentTemp);
            if ((lastTemp!=null)&&(Math.abs(lastTemp-currentTemp)>max_diff)){
                return "报警";
            }
            return "null";
        }
        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
