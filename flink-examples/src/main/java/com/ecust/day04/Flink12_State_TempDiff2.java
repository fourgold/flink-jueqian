package day04;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink12_State_TempDiff2 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流,转换为JavaBean,同时按照sensor_id分组
        KeyedStream<SensorReading, Tuple> sensorReadingTupleKeyedStream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                    }
                })
                .keyBy("id");

        //3.使用RichFunction实现状态编程,如果同一个传感器连续两次温度差值超过10度,则输出报警信息
        SingleOutputStreamOperator<String> result = sensorReadingTupleKeyedStream.map(new MyTempDiffRichFunc(10.0));

        //4.打印数据
        result.print();

        //5.执行
        env.execute();

    }

    public static class MyTempDiffRichFunc extends RichMapFunction<SensorReading, String> {

        private Double maxDiff;

        private ValueState<Double> tempState;

        public MyTempDiffRichFunc(Double maxDiff) {
            this.maxDiff = maxDiff;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class));
        }

        @Override
        public String map(SensorReading value) throws Exception {

            //获取状态数据
            Double lastTemp = tempState.value();

            //更新状态
            tempState.update(value.getTemp());

            //判断是否为第一条数据
            if (lastTemp != null && (Math.abs(lastTemp - value.getTemp()) > maxDiff)) {
                //输出报警信息
                return "连续两次温度差值超过" + maxDiff + "度";
            }

            //如果为第一条数据或者差值不超过maxDiff
            return "";
        }
    }

}
