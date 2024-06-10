package com.wxj.steaming.state.operator;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @Author: xingjian wang
 * @Date: 2024/6/4 14:37
 * @Description: 水位超过指定的阈值发送告警，阈值可以动态修改。
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 数据流
        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction());


        // 配置流（用来广播配置）
        DataStreamSource<String> configDs = env.socketTextStream("localhost", 7777);

        // TODO 1、将 配置流 广播
        MapStateDescriptor<String, Integer> mapDescriptor = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> broadcastDs = configDs.broadcast(mapDescriptor);

        // TODO 2、把 数据流 和广播后的 配置流 connect
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDs.connect(broadcastDs);

        // TODO 3、调用 process
        sensorBCS.process(new BroadcastProcessFunction<WaterSensor, String, String>() {
            /**
             * // TODO 5、数据流 的处理方法
             *
             * 注意：数据流只能读取广播状态！
             *
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(WaterSensor value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 通过上下文获取广播状态，取出里面的值（只读）
                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapDescriptor);
                // 必须判断一下广播状态是否有数据，避免第一条过来的数据是数据流的记录，导致拿不到阈值（这种给一个默认值 10）
                Integer threshold = broadcastState.get("threshold") == null? 10: broadcastState.get("threshold");
                if (value.getVc()>threshold) {
                    out.collect(value + "：水位超过指定阈值 "+ threshold);
                }
            }

            /**
             * TODO 4、广播后的 配置流 的处理方法
             *
             * 注意：只有广播流才能修改广播状态！
             *
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                // 通过上下文，往广播状态里写入数据
                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapDescriptor);
                broadcastState.put("threshold", Integer.valueOf(value));
            }
        }).print();


        env.execute();
    }
}
