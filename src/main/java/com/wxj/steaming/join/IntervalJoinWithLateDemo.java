package com.wxj.steaming.join;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/1 14:08
 * @Description: TODO
 */
public class IntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds1 = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((r, ts) -> r.getTs() * 1000)
                );


        SingleOutputStreamOperator<WaterSensor> ds2 = env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((r, ts) -> r.getTs() * 1000)
                );

        /**
         * TODO interval join：处理乱序流的迟到数据
         *
         *     1、只支持 事件时间，实现 inner join
         *     2、指定 上下界的偏移，可正可负
         *     3、process 只能处理关联上的数据
         *     4、关联后的watermark，取决于两条流中最小的watermark
         *     5、如果 当前数据的事件时间< 当前的watermark，就是迟到数据，主流的process不会处理
         *          => 可以 between后，将 左流或右流 的迟到数据放入 侧输出流。
         */
        OutputTag<WaterSensor> leftTag = new OutputTag<>("left-late", TypeInformation.of(WaterSensor.class));
        OutputTag<WaterSensor> rightTag = new OutputTag<>("right-late", TypeInformation.of(WaterSensor.class));
        SingleOutputStreamOperator<String> res = ds1.keyBy(WaterSensor::getId).intervalJoin(ds2.keyBy(WaterSensor::getId))
                .between(Time.seconds(-2), Time.seconds(2))
                // TODO 1、左流迟到数据
                .sideOutputLeftLateData(leftTag)
                // TODO 2、右流迟到数据
                .sideOutputRightLateData(rightTag)
                .process(new ProcessJoinFunction<WaterSensor, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor left, WaterSensor right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left.toString() + "<--->" + right.toString());
                    }
                });

        res.print("主流");
        res.getSideOutput(leftTag).printToErr("左流迟到数据");
        res.getSideOutput(leftTag).printToErr("右流迟到数据");

        env.execute();
    }
}
