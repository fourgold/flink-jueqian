package day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class FlinkSQL03_Source_File {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.构建文件的连接器
        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("sensorTable");

        //3.读取数据创建表
        Table sensorTable = tableEnv.from("sensorTable");

        //4.TableAPI
        Table tableResult = sensorTable.select("id,temp").where("id ='sensor_1'");

        //5.SQL方式
        Table sqlResult = tableEnv.sqlQuery("select id,ts from sensorTable where id ='sensor_7'");

        //6.转换为流输出
        tableEnv.toRetractStream(tableResult, Row.class).print("Table");
        tableEnv.toRetractStream(sqlResult, Row.class).print("SQL");

        //7.执行
        env.execute();
    }

}
