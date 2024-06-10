package com.wxj.steaming.watermark;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/31 16:05
 * @Description: TODO
 */
public class GenPunctuatedWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(2000);  // 断点式watermark周期设置无效

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction());

        // TODO 指定 watermark 分配策略
        SingleOutputStreamOperator<WaterSensor> dsWithWatermark = ds.assignTimestampsAndWatermarks(
                // TODO 1.3 自定义生成 watermark
                WatermarkStrategy
                        // 直接返回自定义WatermarkGenerator即可
                        .<WaterSensor>forGenerator(ctx -> new MyPunctuatedWatermarkGenerator<>(3 * 1000))
                        // 从数据中提取时间戳
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                            System.out.println("数据=" + element + ", recordTs=" + recordTimestamp);
                            return element.getTs() * 1000L;
                        })

                // TODO 1.4 不生成watermark
                // 可以用于完全基于 处理时间 的程序中
//                WatermarkStrategy.noWatermarks()
        );

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

    /**
     * 自定义 断点式 watermark 生成策略
     * 可以参考 BoundedOutOfOrdernessWatermarks 等系统自带类的写法，这里基本模仿官方的写法做演示
     *
     * @param <WaterSensor>
     */
    public static class MyPunctuatedWatermarkGenerator<WaterSensor> implements WatermarkGenerator<WaterSensor> {
        private long maxTs;
        private long delayTs;

        public MyPunctuatedWatermarkGenerator(long delayTs) {
            this.delayTs = delayTs;
            this.maxTs = Long.MIN_VALUE + delayTs + 1;
        }

        /**
         * 每条数据过来都会调用一次，用来提取最大事件时间并且保存下来
         *
         * TODO 断点式 生成watermark 的发送逻辑在该方法实现
         *
         * @param event
         * @param eventTimestamp 提取的数据的事件事件
         * @param output
         */
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(maxTs, eventTimestamp);
            output.emitWatermark(new Watermark(maxTs - delayTs - 1));  // 不同！
            System.out.println("调用 MyWatermarkGenerator.onEvent() 方法，获取最大时间戳："+maxTs+"; 生成 watermark："+(maxTs - delayTs - 1));
        }

        /**
         * 断点式 无需调用该方法
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}
