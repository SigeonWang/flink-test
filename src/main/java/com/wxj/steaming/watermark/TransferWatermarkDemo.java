package com.wxj.steaming.watermark;

import com.wxj.Partitioner.IntegerPartitioner;
import com.wxj.Partitioner.StringPartitioner;
import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * @Author: xingjian wang
 * @Date: 2024/5/31 12:13
 * @Description: 多并行度下的 watermark 传递
 */
public class TransferWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//                .getExecutionEnvironment();

        // 演示多并行度下 watermark 的传递
        // socketTextStream() source算子并行度是1，默认并行度2，所以map()算子并行度是2，也就是记录会以rebalance的方式依次发往两个map子任务。
        env.setParallelism(2);

        // 输入：数字
        env.socketTextStream("localhost", 6666)
                // TODO 自定义分区器
                // 分区：自定义的 IntegerPartitioner 控制输入数字的奇偶，让数据都进入其中一个子任务
                .partitionCustom(new IntegerPartitioner(), r -> r)
                .map(Integer::parseInt)
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Integer>forMonotonousTimestamps()
                            // 从数据中提取时间戳
                            .withTimestampAssigner((r, ts) -> r * 1000L)
                            .withIdleness(Duration.ofSeconds(5))  // 设置空闲等待时间，避免某个上游任务一直不推进watermark导致整个系统处理进度停滞。
                )
                // 分组：奇数一组，偶数一组;
                // 窗口：事件时间滚动窗口
                .keyBy(r -> r % 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer key, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        long cnt = elements.spliterator().estimateSize();  // 数据的估计条数
                        out.collect("key="+key+"的窗口[" +windowStart+", "+windowEnd+"] 包含 "
                                +cnt+" 条数据 => " + elements.toString());
                    }
                })
                .print();

        env.execute();
    }
}
