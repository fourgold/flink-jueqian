package practice;


import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Properties;

// 从Kafka读取传感器数据
// 统计每个传感器发送温度的次数存入MySQL(a表)
// 如果某个传感器温度连续10秒不下降
// 则输出报警信息到侧输出流并存入MySQL(b表)
public class Flink05_WordCount_OnTimer_SideOutPut_State_JdbcSink1 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取kafka数据源
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"BigDataTest");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        DataStreamSource<String> kafka = env.addSource(new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), properties));
        kafka.print("kafka");

        //3.将kafka消费得来的消息转换为javaBean对象并按照id分组
        SingleOutputStreamOperator<SensorReading> sensorOpt = kafka.map(new MyMapFunc10());
        KeyedStream<SensorReading, String> keyedStream = sensorOpt.keyBy(SensorReading::getId);

        //4.使用process状态编程统计数量,然后使用计时器做报警器
        SingleOutputStreamOperator<String> process = keyedStream.process(new MyProcess(10));

        process.print("主流");

        process.getSideOutput(new OutputTag<String>("alertSideOut"){}).print("测流");

        process.addSink(new MyJDBCSink1("main"));
        process.getSideOutput(new OutputTag<String>("alertSideOut"){}).addSink(new MyJDBCSink1("side"));

        env.execute();
    }

    //转换成javaBean
    private static class MyMapFunc10 implements MapFunction<String, SensorReading> {
        @Override
        public SensorReading map(String value) throws Exception {
            String[] words = value.split(",");
            return new SensorReading(words[0],Long.parseLong(words[1]),Double.parseDouble(words[2]));
        }
    }

    private static class MyProcess extends KeyedProcessFunction<String, SensorReading, String> {
        private Integer interval;
        //统计个数状态变量
        private ValueState<Integer> count;

        //记录定时时间状态->用于关闭与删除定时器
        private ValueState<Long> tsState;

        //记录温度状态->用于检测两次温度的改变
        private ValueState<Double> tempState;

        private SimpleDateFormat simpleDateFormat;

        public MyProcess(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class, 0));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts", Long.class));
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp", Double.class,Double.MIN_VALUE));
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            //更新计数状态变量 并使用主流统计
            int curCount = count.value() + 1;
            count.update(curCount);
            out.collect(ctx.getCurrentKey()+","+curCount);

            //根据温度的改变创建计时器
            Double lastTemp = tempState.value();
            Double curTemp = value.getTemp();
            tempState.update(curTemp);

            Long lastTiming = tsState.value();
            long curTiming = ctx.timerService().currentProcessingTime() + interval*1000L;

            if (curTemp >= lastTemp && lastTiming == null){
                //说明是第一次判断,开启定时
                ctx.timerService().registerProcessingTimeTimer(curTiming);
                tsState.update(curTiming);
            }else if (curTemp < lastTemp && lastTiming != null){
                ctx.timerService().deleteProcessingTimeTimer(lastTiming);
                tsState.clear();
                ctx.timerService().registerProcessingTimeTimer(curTiming);
                tsState.update(curTiming);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            String format = simpleDateFormat.format(timestamp);

            //输出定时器报警信息
            ctx.output(new OutputTag<String>("alertSideOut"){},format+","+ctx.getCurrentKey()+","+interval+","+tempState.value());
//            tsState.clear();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+interval*1000L);
            tsState.update(ctx.timerService().currentProcessingTime()+interval*1000L);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

    private static class MyJDBCSink1 extends RichSinkFunction<String> {
        private String mode;
        private Connection connection;
        private PreparedStatement preparedStatement;

        public MyJDBCSink1(String mode) {
            this.mode = mode;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");

            if ("main".equals(mode)) {
                preparedStatement = connection.prepareStatement("insert into sensor_id (id,ct) values(?,?) on duplicate key update ct=?");
            }else if("side".equals(mode)){
                preparedStatement = connection.prepareStatement("insert into sensor_alert (ts,id,inter,temp) values(?,?,?,?)");
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {

            if ("side".equals(mode)) {
                String[] fields = value.split(",");
                preparedStatement.setString(1,fields[0]);
                preparedStatement.setString(2,fields[1]);
                preparedStatement.setString(3,fields[2]);
                preparedStatement.setString(4,fields[3]);
                System.out.println("sss");
            }else if ("main".equals(mode)){
                String[] fields = value.split(",");
                preparedStatement.setString(1,fields[0]);
                preparedStatement.setString(2,fields[1]);
                preparedStatement.setString(3,fields[1]);
                System.out.println("you need enter '[side]' or '[main]',default is [main]");
            }

            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
