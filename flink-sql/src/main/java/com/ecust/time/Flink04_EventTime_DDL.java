package com.ecust.time;

import com.sun.org.apache.xerces.internal.impl.dv.dtd.ENTITYDatatypeValidator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author JueQian
 * @create 01-22 16:57
 * todo 会报错注意
 * 使用DDL处理文件事件的时间问题?????报错 2021-01-21T11:33:13
 */
public class Flink04_EventTime_DDL {
    public static void main(String[] args) throws Exception {

        //0x0 获取执行环境 todo 使用新版环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //0x1 使用DDL语句构建DDL的连接器 todo 注意地址
        String sinkDDL = "create table dataTable (" +
                " id varchar(20) not null, " +
                " ts bigint, " +
                " temp double, " +
                " rt as to_timestamp(from_unixtime((ts/1000),'yyyy-MM-dd HH:mm:ss')),"+
                " watermark for rt as rt - interval '1' second"+
                ") with (" +
                " 'connector.type' = 'filesystem', " +
                " 'connector.path' = 'data/sensor.txt', " +
                " 'format.type' = 'csv')";
        tableEnv.sqlUpdate(sinkDDL);


        //0x2 读取数据创建表
        Table sensorTable = tableEnv.from("dataTable");

        //0x4 打印表的信息
        sensorTable.printSchema();
        tableEnv.toAppendStream(sensorTable, Row.class).print();

        //0x5 执行
        env.execute();

    }
}
