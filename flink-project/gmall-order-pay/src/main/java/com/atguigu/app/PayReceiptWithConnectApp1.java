package com.atguigu.app;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.ReceiptEvent;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 实时对账流
 *
 */

public class PayReceiptWithConnectApp1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取流,两个流,数据不一样,类型不一样用connect(双流-join)
        KeyedStream<OrderEvent, String> source1 = env.readTextFile("./input/OrderLong.csv").map(line -> {
            String[] fields = line.split(",");
            long orderId = Long.parseLong(fields[0]);
            String eventType = fields[1];
            String txID = fields[2];
            long eventTime = Long.parseLong(fields[3]);
            return new OrderEvent(orderId, eventType, txID, eventTime);
        })
                .filter(data -> "pay".equals(data.getEventType()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getEventTime() * 1000L;
            }
        }).keyBy(OrderEvent::getTxId);

        KeyedStream<ReceiptEvent, String> source2 = env.readTextFile("./input/ReceiptLog.csv").map(line -> {
            String[] fields = line.split(",");
            return new ReceiptEvent(fields[0], fields[1], Long.parseLong(fields[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
            @Override
            public long extractAscendingTimestamp(ReceiptEvent element) {
                return element.getTimestamp() * 1000L;
            }
        }).keyBy(ReceiptEvent::getTxId);








    }

}
