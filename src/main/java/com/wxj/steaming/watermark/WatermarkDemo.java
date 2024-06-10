package com.wxj.steaming.watermark;

import com.wxj.Partitioner.StringPartitioner;
import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/31 12:13
 * @Description: TODO
 */
public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);  // flink1.12 开始默认
//        env.getConfig().setAutoWatermarkInterval(200);  // watermark 生成间隔，默认200ms，一般不建议修改

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction());

        // TODO 指定 watermark 分配策略
        // 步骤：
        //     1）指定 watermark生成策略
        //     2）指定 时间戳生成策略
        //     3）使用 基于事件时间 的窗口
        SingleOutputStreamOperator<WaterSensor> dsWithWatermark = ds.assignTimestampsAndWatermarks(
                // TODO 1.1 有序流生成watermark
//                WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
//                        // 从数据中提取时间戳
//                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
//                            System.out.println("数据=" + element + ", recordTs=" + recordTimestamp);
//                            // 返回的时间戳格式需要是ms
//                            return element.getTs() * 1000L;
//                        })
                // TODO 1.2 无序流生成watermark
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        // 从数据中提取时间戳
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                            System.out.println("数据=" + element + ", recordTs=" + recordTimestamp);
                            return element.getTs() * 1000L;
                        })
        );

        // TODO 必须配合 基于事件时间 的窗口
        // 问题：单并行度下，ts=10数据到来process()就会打印结果；多并行度下，ts=10数据到来process()并不会打印结果 ?
        // 原因：多并行度下 watermark 传递的原理
        //      如果接收到上游多个子任务的watermark，算子会取 最小的watermark 作为当前节点的处理进度；
        //      将 watermark 向下游广播；
        dsWithWatermark.keyBy(WaterSensor::getId)
                // 指定 事件时间滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        long cnt = elements.spliterator().estimateSize();  // 数据的估计条数
                        out.collect("key="+s+"的窗口[" +windowStart+", "+windowEnd+"] 包含 "
                                +cnt+" 条数据 => " + elements.toString());
                    }
                })
                .print();

        env.execute();
    }
}
