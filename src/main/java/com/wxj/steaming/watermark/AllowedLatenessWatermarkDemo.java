package com.wxj.steaming.watermark;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/31 18:54
 * @Description: TODO
 */
public class AllowedLatenessWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * TODO 延迟数据的处理
         */
        OutputTag<WaterSensor> lateDataTag = new OutputTag<>("lateData", TypeInformation.of(WaterSensor.class));
        SingleOutputStreamOperator<String> res = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        // TODO 处理1：推迟 watermark 推进
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                // 从数据中提取时间戳
                                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                                    System.out.println("数据=" + element + ", recordTs=" + recordTimestamp);
                                    return element.getTs() * 1000L;
                                })
                )
                .keyBy(WaterSensor::getId)
                // 指定 事件时间滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // TODO 处理2：允许窗口延迟关闭（这期间窗口对延迟的数据做增量计算，延迟过后窗口才会销毁）
                .allowedLateness(Time.seconds(5))
                // TODO 处理3：使用 侧输出流（窗口关闭后的迟到数据，可以通过此方式处理）
                .sideOutputLateData(lateDataTag)
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        long cnt = elements.spliterator().estimateSize();  // 数据的估计条数
                        out.collect("key=" + s + "的窗口[" + windowStart + ", " + windowEnd + "] 包含 "
                                + cnt + " 条数据 => " + elements.toString());
                    }
                });
        res.print("主流");

        // 打印侧输出流
        res.getSideOutput(lateDataTag).printToErr("关窗后的迟到数据");

        env.execute();
    }

    /**
     * 探究这个过程，假设
     * 输入："s1,1,1" "s1,5,1" "s1,10,1" "s1,11,1" "s1,8,1" "s1,9,1" "s1,13,1" "s1,4,1" "s1,14,1" "s1,6,1" "s1,18,1" "s1,19,1" "s1,2,1"
     *      当 "s1,13,1" 达到时，watermark=13-3=10>=窗口最大值10，触发窗口计算，输出 ["s1,1,1" "s1,5,1" "s1,8,1" "s1,9,1" ]
     *      "s1,4,1"、"s1,6,1" 分别到达时，作为窗口关闭前的迟到数据，继续做增量计算并输出
     *      当 "s1,18,1" 达到时，watermark=18-3=15>=窗口最大值10+允许迟到时间5，窗口关闭。
     *      "s1,2,1" 到达时，作为窗口关闭后的迟到数据，通过侧输出流输出
     *
     * 思考：如果 watermark 等待3s，窗口允许迟到2s, 为什么不直接 watermark等待5s 或者 窗口允许迟到5s?
     *    =》 watermark 等待时间不会设太大，否则会影响的计算延迟！
     *          如果3s的数据来 ==》 窗口第一次触发计算和输出，13-3=10s
     *          如果5s的数据来 ==》 窗口第一次触发计算和输出，15-5=10s
 *        =》 窗口允许迟到，是对大部分迟到数据的处理，尽量让结果准确
     *          如果只设置允许迟到5s，那么就会导致频繁重新输出！
     *
     * TODO 设置经验
     * 1、watermark 等待时间，设置一个不算特别大的，一般是秒级，需要在 乱序 和 延迟 取舍；
     * 2、设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端小部分迟到很久的数据，不管；
     * 3、极端小部分迟到很久的数据，放到侧输出流。获取到之后可以做各种处理。
     *
     */
}
