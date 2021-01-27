package day05;

import bean.SensorReading;
import day02.Flink08_Transform_Map;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * 同一个传感器若出现连续10秒温度不下降,报警
 */
public class Flink01_State_OnTimer_TempNotDesc1 {

    public static void main(String[] args) throws Exception {
        //todo 1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //todo 2.读取端口数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        //todo 3.转换为样例类
        SingleOutputStreamOperator<SensorReading> mapSensor = source.map(new Flink08_Transform_Map.MyMapFunc());
        //todo 4. 分组
        KeyedStream<SensorReading, String> keyedStream = mapSensor.keyBy(SensorReading::getId);
        //todo 5.报警
        SingleOutputStreamOperator<String> result = keyedStream.process(new MyKeyedProcess(10));

        //打印执行
        result.print();
        env.execute();
    }

    private static class MyKeyedProcess extends KeyedProcessFunction<String,SensorReading,String> {
        //时间间隔
        private Integer interval;

        //声明状态用于存储每一次的温度值
        private ValueState<Double> tempState;

        //声明状态用于存放定时器时间
        private ValueState<Long> tsState;

        public MyKeyedProcess(int time) {
            this.interval=time;
        }

        //设置状态
        @Override
        public void open(Configuration parameters) throws Exception {
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp", Double.class,Double.MIN_VALUE));
            tsState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // get 上一次 温度值
            Double lastTemp = tempState.value();

            //获取上次状态记录时间
            Long ts = tsState.value();

            //获取当前温度
            Double curTemp = value.getTemp();

            //状态更新温度
            tempState.update(curTemp);

            //第一条数据注册定时器,温度上升,并且状态为null
            if (curTemp > lastTemp && ts==null){

                //获取当前时间
                long curTs = ctx.timerService().currentProcessingTime() + interval * 1000L;

                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(curTs);
                tsState.update(curTs);

            }else if (curTemp < lastTemp && ts !=null){

                //删除定时器
                ctx.timerService().deleteProcessingTimeTimer(ts);
                tsState.clear();
            }
        }

        //定时器触发任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            out.collect(ctx.getCurrentKey()+" : 连续10秒温度没有下降!!!");

            //清空时间状态 响了,之后就删掉
            tsState.clear();
        }
    }
}
