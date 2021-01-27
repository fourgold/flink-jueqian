package day05;

import bean.SensorReading;
import day02.Flink08_Transform_Map;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Jinxin Li
 * @create 2020-12-15 10:20
 * 需求,若是同一个传感器,连续10秒内温度不下降,就会报警
 */
public class Flink01_State_OnTimer_TempNotDesc2 {

    public static void main(String[] args) throws Exception {

        //环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000L);
        //获取socket并转换为javaBean对象
        //4.1 固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        //4.2 失败率重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(50),Time.seconds(5)));


        SingleOutputStreamOperator<SensorReading> mapSensorReading
                = env.socketTextStream("hadoop102", 9999).map(new Flink08_Transform_Map.MyMapFunc());
        //将javaBean对象分组
        KeyedStream<SensorReading, String> keyedStream = mapSensorReading.keyBy(SensorReading::getId);

        //分组使用process实现定时10秒报警逻辑
        keyedStream.process(new MyProcess2(10L)).print();

        //执行
        env.execute();

    }

    private static class MyProcess2 extends KeyedProcessFunction<String,SensorReading,String> {

        //用于传输数据
        private long interval;
        ValueState<Double> tempState;
        ValueState<Long> tsState;
        //构造函数
        public MyProcess2(long interval) {
            this.interval = interval;
        }

        //起始函数
        @Override
        public void open(Configuration parameters) throws Exception {
            //获取状态,存储温度用于相互比较
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("tempState", Double.class, Double.MIN_VALUE));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState", Long.class));
        }

        //启动
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("10秒内没有降");
            tsState.clear();

        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            //获得上次温度
            Double lastTemp = tempState.value();

            //获取当前温度
            Double curTemp = value.getTemp();

            //更新温度
            tempState.update(curTemp);

            //获取上次时间
            Long lastTs = tsState.value();


            //定时
            if (curTemp > lastTemp && lastTs==null){
                //设定定时时间
                long timingTs = ctx.timerService().currentProcessingTime() + 1000L * interval;
                ctx.timerService().registerProcessingTimeTimer(timingTs);

                tsState.update(timingTs);

            }else if (curTemp<=lastTemp){
                ctx.timerService().deleteProcessingTimeTimer(lastTs);

                tsState.clear();
            }

        }
    }
}
