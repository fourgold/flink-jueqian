package day06;

import bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL01_Test2 {

    /**
     * 读取端口数据使用sql来读取结果
     * @param args
     * @throws Exception
     * 字段从哪里来?
     * sensor1-
     * 为什么不能追加
     */
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        env.setParallelism(1);

        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> map = source.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        //3.对流进行注册
       tableEnv.createTemporaryView("sensor",map);

        //4.SQL方式实现查询 字段从何处判断,javaBean么?
//        Table sqlQuery = tableEnv.sqlQuery("select id,temp from sensor where id ='sensor_1'");
        Table sqlQuery = tableEnv.sqlQuery("select id,count(id) from sensor group by id");


        //5.TableAPI的方式 可以from(可以从现有的注册好的临时表中创建一张表)
        Table table = tableEnv.fromDataStream(map);
        Table result = table.groupBy("id").select("id,id.count");
        //todo 上述暂时不能追加的表
//        Table result = table.select("id").where("id='sensor_1'");


        //6.将表转换为流进行输出 row.class不懂
        /*DataStream<Row> stream = tableEnv.toAppendStream(sqlQuery, Row.class);
        DataStream<Row> dataStream = tableEnv.toAppendStream(result, Row.class);*/

        DataStream<Tuple2<Boolean, Row>> stream = tableEnv.toRetractStream(sqlQuery, Row.class);
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(result, Row.class);

        //row getString[索引]
        stream.print("sql");
        dataStream.print("API");

        //执行
        env.execute();
    }

}
