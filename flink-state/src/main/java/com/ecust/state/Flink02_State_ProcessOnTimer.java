package com.ecust.state;


import com.ecust.state.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author JueQian
 * @create 01-18 9:01
 * 需求:监测传感器温度,在10秒内不下降,使用processAPI.onTimer
 */
public class Flink02_State_ProcessOnTimer {
    public static void main(String[] args) throws Exception {
        //0x0 获取执行环境与端口数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //0x1 将数据源转换为SensorReading对象
        SingleOutputStreamOperator<SensorReading> sensorStream = source.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                String id = fields[0];
                long ts = Long.parseLong(fields[1]);
                double temp = Double.parseDouble(fields[2]);
                return new SensorReading(id, ts, temp);
            }
        });

        //0x2 分组,然后使用processAPi定时统计计算
        sensorStream.keyBy(SensorReading::getId).process(new MyKeyedProcessFunction(10));

        //0x3 执行计划
        env.execute();
    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String,SensorReading,SensorReading>{
        //间隔属性
        private long interval;
        //状态属性
        private ValueState<Double> lastTemp;
        //定时器属性
        private ValueState<Long> timer;

        public MyKeyedProcessFunction() {
        }

        //定义构造器外部传入参数
        public MyKeyedProcessFunction(long interval) {
            this.interval=interval;
        }

        //生命周期函数
        @Override
        public void open(Configuration parameters) throws Exception {
            //获取上下文对象
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));

            //获取定时器记录状态
            timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
            //获取现在传感器的温度
            Double temp = sensorReading.getTemp();

            //获取上次传感器的温度
            Double last_temp = lastTemp.value();

            //获取当前时间
            long ts = context.timerService().currentProcessingTime();

            if (last_temp!=null && temp<last_temp){
                Long lastTs = timer.value();
                context.timerService().deleteProcessingTimeTimer(lastTs);
                //注册新的10秒的定时器
                context.timerService().registerProcessingTimeTimer(ts+interval*1000L);
                //更新状态
                timer.update(ts+interval*1000L);
            }else if (last_temp==null){
                //注册新的10秒的定时器
                context.timerService().registerProcessingTimeTimer(ts+interval*1000L);
                //更新状态
                timer.update(ts+interval*1000L);
            }
            lastTemp.update(temp);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            System.out.println("温度在"+interval+"秒内没有下降");
        }
    }
}
