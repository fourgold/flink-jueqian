package com.ecust.time;

import com.ecust.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author JueQian
 * @create 01-22 16:57
 * todo 会报错注意
 */
public class Flink03_EventTime_ConnectSource {
    public static void main(String[] args) {

        //0x0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //0x1 构建文件的连接器,并添加文件的事件处理时间
//        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        tableEnv.connect(new FileSystem().path("data/sensor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT()).rowtime(new Rowtime()
                        .timestampsFromField("ts").watermarksPeriodicBounded(1000))
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sensorTable");
        //0x2 读取数据创建表
        Table sensorTable = tableEnv.from("sensorTable");

        //0x4 打印表的信息
        sensorTable.printSchema();

        //0x5 执行

    }
}
