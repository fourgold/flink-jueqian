package day06;


import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author Jinxin Li
 * @create 2020-12-16 16:12
 */
public class FlinkSQL05_Sink_File2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.从端口读取数据,并且转换为样例类数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102",9999);

        SingleOutputStreamOperator<SensorReading> sensorDS = source.map(line -> {
            String[] words = line.split(",");
            return new SensorReading(words[0],Long.parseLong(words[1]), Double.parseDouble(words[2]));
        });

        Table table = tableEnv.fromDataStream(sensorDS,"id,ts,temp");
        Table sqlQuery = tableEnv.sqlQuery("select id,ts,temp from "+table);

        //3.创建文件连接器,根据文件连接器创建schema表图解,然后创建临时表存储数据
        tableEnv.connect(new FileSystem()
                .path("./outputFile"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("sensorOutput");
        //4.写入外部文件系统
        tableEnv.insertInto("sensorOutput",sqlQuery);

        env.execute();
    }
}
