package day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author Jinxin Li
 * @create 2020-12-16 14:36
 */
public class FlinkSQL03_Source_File1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //构建文件的连接器
        tableEnv.connect(new FileSystem().path("./sensor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sensorTable");

        //通过连接器直接创建表
        Table sensorTable = tableEnv.from("sensorTable");

        //TableAPI
        Table tableResult = sensorTable.select("id,temp").where("id='sensor_1'");

        //sql
        Table sqlResult = tableEnv.sqlQuery("select id,ts from sensorTable where id = 'sensor_7'");

        //转换为输出
        tableEnv.toRetractStream(tableResult, Row.class).print("apl");
        tableEnv.toRetractStream(sqlResult,Row.class).print("sql");

        env.execute();
    }
}
