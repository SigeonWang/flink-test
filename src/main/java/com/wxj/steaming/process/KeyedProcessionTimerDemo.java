package com.wxj.steaming.process;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/2 11:43
 * @Description: TODO
 */
public class KeyedProcessionTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction());
        // 使用处理时间时，watermark 不生效。
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                                .withTimestampAssigner((r, ts) -> r.getTs() * 1000)
//                );

        /**
         * TODO 处理时间定时器
         */
        ds.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    /**
                     * TimerService会以 键 和 注册的时间戳 为标准，对定时器进行去重
                     *      所以对于每个key，只要注册时 系统时间 不同，都会注册一个单独的定时器！
                     *
                     */
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        String key = ctx.getCurrentKey();
                        // 获取定时器
                        TimerService timer = ctx.timerService();
                        // 注册定时器：处理时间
                        long ts = timer.currentProcessingTime();
                        timer.registerProcessingTimeTimer(ts + 5 * 1000);  // 注册key第一次达到时间之后5s的处理时间定时器
                        System.out.println("当前key "+key+", 当前处理时间 "+ts+", 注册一个 +5s 的定时器");
                    }

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
