package practice;

import bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink05_WordCount_OnTimer_SideOutPut_State_JdbcSink {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据转换为JavaBean并按照ID分组
        KeyedStream<SensorReading, String> keyedStream = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                })
                .keyBy(SensorReading::getId);

        //3.使用ProcessFunction实现我们所需的功能(计算WordCount输出到主流,报警信息输出到侧输出流)
        SingleOutputStreamOperator<String> result = keyedStream.process(new MyKeyedProcessFunc());

        //4.将主流输出到MySQL
        result.addSink(new MyJdbcSink("INSERT INTO a(id,ct) VALUES(?,?) ON DUPLICATE KEY UPDATE ct=?;", 3));

        //5.获取侧输出流数据输出到MySQL
       result.getSideOutput(new OutputTag<String>("SideOutPut") {
        }).addSink(new MyJdbcSink("INSERT INTO b(str) VALUES(?)", 1));

        //6.打印测试
        result.print("result");
        result.getSideOutput(new OutputTag<String>("SideOutPut") {
        }).print("SideOutPut");

        //7.执行
        env.execute();

    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<String, SensorReading, String> {

        //声明状态
        private ValueState<Double> tempState;
        private ValueState<Long> tsState;
        private ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class, Double.MIN_VALUE));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class, 0L));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            //1.获取所有状态信息
            Double lastTemp = tempState.value();
            Long ts = tsState.value();
            Long count = countState.value();

            //2.获取当前数据中的温度信息
            Double curTemp = value.getTemp();

            //3.更新状态
            tempState.update(curTemp);
            countState.update(count + 1L);

            //4.WordCount逻辑输出
            out.collect(value.getId() + "," + (count + 1));

            //5.温度上升并且当前没有定时器时注册定时器
            if (curTemp > lastTemp && ts == null) {
                //获取定时器时间
                long curTs = ctx.timerService().currentProcessingTime() + 10000L;
                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(curTs);
                //更新时间状态
                tsState.update(curTs);
            }

            //6.如果温度下降并且已经存在了定时器,则删除定时器
            else if (curTemp < lastTemp && ts != null) {
                //删除定时器
                ctx.timerService().deleteProcessingTimeTimer(ts);
                //清空状态
                tsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //将报警信息输出到侧输出流
            ctx.output(new OutputTag<String>("SideOutPut") {
                       },
                    ctx.getCurrentKey() + "在" + timestamp + "温度已经连续10秒没有下降！！！");
            //清空状态
            tsState.clear();
        }
    }


    public static class MyJdbcSink extends RichSinkFunction<String> {

        //定义属性信息
        private String sql;
        private int paraNum;

        public MyJdbcSink(String sql, int paraNum) {
            this.sql = sql;
            this.paraNum = paraNum;
        }

        //声明JDBC连接
        private Connection connection;
        //声明预编译SQL
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取JDBC连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "000000");
            //预编译SQL
            preparedStatement = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {

            //按照","分割
            String[] fields = value.split(",");

            //给预编译SQL赋值
            for (int i = 0; i < paraNum; i++) {
                if (i == 2) {
                    preparedStatement.setObject(i + 1, fields[i - 1]);
                } else {
                    preparedStatement.setObject(i + 1, fields[i]);
                }
            }

            //执行SQL
            preparedStatement.execute();

        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }

}

