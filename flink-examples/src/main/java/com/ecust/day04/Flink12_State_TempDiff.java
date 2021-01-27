package day04;

import bean.SensorReading;
import day02.Flink08_Transform_Map;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api .environment.StreamExecutionEnvironment;

/**
 * @author Jinxin Li
 * @create 2020-12-14 16:52
 */
public class Flink12_State_TempDiff {
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

        public MyTempDiff(double maxDiff) {
            this.maxDiff = maxDiff;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class));
        }

        @Override
        public String map(SensorReading value) throws Exception {
            Double lastTemp = state.value();
            state.update(value.getTemp());
            if (lastTemp!=null&&(Math.abs(lastTemp-value.getTemp())>maxDiff)){
                //更新状态
                return "相差超过10度";
            }

            return "\b";
        }
    }
}
