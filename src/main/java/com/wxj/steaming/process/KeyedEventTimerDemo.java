package com.wxj.steaming.process;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/2 10:52
 * @Description: TODO
 */
public class KeyedEventTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((r, ts) -> r.getTs() * 1000)
                );

        /**
         * TimerService：定时器
         */
        ds.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    /**
                     * TODO 1、注册定时器
                     * 注意：
                     *    数据来一条调用一次；
                     *    但是 TimerService会以 键 和 时间戳(事件时间的watermatk / 处理时间的系统时间) 为标准，对定时器进行去重（只调用一次onTimer()）
                     *    因为 watermark 本质也是一条数据（对应数据处理完后生成的特殊数据），且 process 一次只能处理一条数据：
                     *          所以 processElement() 方法拿到的 watermark 其实是上一条数据处理完后生成的。
                     *
                     * @param value 数据
                     * @param ctx 上下文
                     * @param out 输出采集器
                     * @throws Exception
                     */
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 获取数据提取的事件时间，如果使用ProcessTime该方法返回null
                        Long ts = ctx.timestamp();
                        String key = ctx.getCurrentKey();

                        // 获取定时器
                        TimerService timer = ctx.timerService();
                        // 1.1）注册定时器：事件时间
                        timer.registerEventTimeTimer(5 * 1000);  // 当事件时间达到注册的事件时间5s时，定时器触发调用onTimer()
                        long wm = timer.currentWatermark();
                        System.out.println("当前key "+key+", 当前事件时间 "+ts+", 当前watermark "+wm+", 注册一个5s的定时器");

                        // 1.1）注册定时器：处理时间
//                        timer.registerProcessingTimeTimer(5);
                        // 1.2）获取当前时间：事件时间 watermark
//                        timer.currentWatermark();
                        // 1.2）获取当前时间：处理时间（系统时间）
//                        timer.currentProcessingTime();
                        // 1.3）删除定时器：事件时间
//                        timer.deleteEventTimeTimer(5);
                        // 1.3）删除定时器：处理时间
//                        timer.deleteProcessingTimeTimer(5);
                    }

                    /**
                     * TODO 2、调用定时器
                     * 当 watermark >= 定时器注册的时间，触发定时器，调用该方法。
                     * 注意 watermark = 上游算子最小watermark - 延迟时间 - 1ms
                     *
                     * @param timestamp 当前时间进展
                     * @param ctx 上下文
                     * @param out 输出采集器
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);

                        String key = ctx.getCurrentKey();
                        System.out.println("当前key "+key+", 现在时间是 "+timestamp+", 定时器触发");
                    }
                })
                .print();
                

        env.execute();
    }
}
