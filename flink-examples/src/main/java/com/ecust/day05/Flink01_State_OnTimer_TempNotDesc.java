package day05;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_State_OnTimer_TempNotDesc {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000L);

        //2.读取端口数据,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                    }
                });

        //3.分组
        KeyedStream<SensorReading, String> keyedStream = sensorDS.keyBy(SensorReading::getId);

        //4.使用ProcessAPI实现10秒温度没有下降则报警逻辑
        SingleOutputStreamOperator<String> result = keyedStream.process(new MyKeyedProcessFunc(10));

        //5.打印
        result.print();

        //6.执行
        env.execute();

    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<String, SensorReading, String> {

        //定义属性,时间间隔
        private Integer interval;

        //声明状态用于存储每一次的温度值
        private ValueState<Double> tempState;

        //声明状态用于存放定时器时间
        private ValueState<Long> tsState;

        public MyKeyedProcessFunc(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //状态初始化
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class, Double.MIN_VALUE));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            //获取上一次温度值
            Double lastTemp = tempState.value();
            //获取时间状态数据
            Long ts = tsState.value();
            //获取当前温度
            Double curTemp = value.getTemp();
            //更新温度状态
            tempState.update(curTemp);

            //温度上升并且时间状态为null,注册定时器
            if (curTemp > lastTemp && ts == null) {
                //获取当前时间
                long curTs = ctx.timerService().currentProcessingTime() + interval * 1000L;
                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(curTs);
                //更新时间状态
                tsState.update(curTs);
            }

            //当温度下降时,删除定时器
            else if (value.getTemp() < lastTemp && ts != null) {
                //删除定时器
                ctx.timerService().deleteProcessingTimeTimer(ts);
                //清空时间状态
                tsState.clear();
            }
        }

        //定时器触发任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //输出数据
            out.collect(ctx.getCurrentKey() + " 连续10秒温度没有下降！！！");
            //清空时间状态
            tsState.clear();
        }
    }
}