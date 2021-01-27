package day04;

import bean.SensorReading;
import day02.Flink08_Transform_Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.management.Sensor;

/**
 * @author Jinxin Li
 * @create 2020-12-14 16:52
 */
public class Flink12_State_TempDiff1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //换转为javaBean然后分组
        SingleOutputStreamOperator<SensorReading> map = source.map(new Flink08_Transform_Map.MyMapFunc());

        KeyedStream<SensorReading, String> keyBy = map.keyBy(SensorReading::getId);
//        map.keyBy(line->line.getId());


        //3.使用richFunction实现状态编程,如果连续同一个传感器,连续两次温度差值超过十度
        //则输出报警
        SingleOutputStreamOperator<String> result = keyBy.map(new MyTempDiff(10.0));
        result.print();
        env.execute();


    }


    private static class MyTempDiff extends RichMapFunction<SensorReading,String> {
        private double maxDiff;

        private  ValueState<Double> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class));
        }

        public MyTempDiff(double maxDiff) {
            this.maxDiff = maxDiff;
        }

        @Override
        public String map(SensorReading value) throws Exception {
            Double lastTemp = state.value();
            if (lastTemp==null){
                //更新状态
                return null;
            }else{
                if ((Math.abs(lastTemp-value.getTemp())>maxDiff)){
                    //输出
                    return "报警";
                }
            }
            state.update(value.getTemp());
            return null;
        }
    }
}
