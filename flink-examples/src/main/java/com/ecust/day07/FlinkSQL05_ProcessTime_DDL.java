package day07;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

public class FlinkSQL05_ProcessTime_DDL {

    public static void main(String[] args) {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        //2.构建文件的DDL
        String sinkDDL = "create table dataTable (" +
                " id varchar(20) not null, " +
                " ts bigint, " +
                " temp double, " +
                " pt AS PROCTIME() " +
                ") with (" +
                " 'connector.type' = 'filesystem', " +
                " 'connector.path' = 'sensor', " +
                " 'format.type' = 'csv')";
        bsTableEnv.sqlUpdate(sinkDDL);

        //3.读取数据创建表
        Table sensorTable = bsTableEnv.from("dataTable");

        //4.打印表的信息
        sensorTable.printSchema();

    }

}
