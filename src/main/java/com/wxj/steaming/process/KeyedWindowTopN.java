package com.wxj.steaming.process;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.*;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/2 12:15
 * @Description: 实时统计一段时间内的出现次数最多的水位。
 *     要求：统计最近10秒钟内出现次数最多的两个水位，并且每5秒钟更新一次。
 * 思路：
 *      开滑动窗口收集传感器的数据，按照不同的水位进行统计，而后汇总排序并最终输出前两名。
 */
public class KeyedWindowTopN {
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
         * TODO 思路二： 使用 KeyedProcessFunction 实现
         *     1、按照 vc 做 keyBy，开窗，分别 count
         *        ==》 增量聚合，计算 count
         *        ==》 全窗口，对计算结果 count 值封装，带上 窗口结束时间 标签（为了让同一个窗口时间范围的计算结果到一起去）
         *     2、对同一个窗口范围的count值进行处理：排序、取前N个
         *        ==》 按照 windowEnd 做 keyBy
         *        ==》 使用 process()， 来一条调用一次，需要先存，分开存，用HashMap,key=windowEnd,value=List
         *        ==》 使用定时器，对用 HashMap 进行 排序、取前N个值
         *
         */
        // 1、按照 vc 分组、开窗、聚合（增量计算+全量打标签）
        // 开窗聚合后，就是普通的流，没有了窗口信息，需要自己打上窗口的标记 windowEnd，便于之后区分窗口
        // 注意：
        //      在 "s1,8,2" 输入后，watermark（8） > 窗口结束时间（5+3-1ms），所以已经触发了窗口函数计算，
        //      但是后续的process()定义的时间触发是windowEnd+1，此时并不会触发，再输入"s1,8,2"则会触发
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowAgg = ds
                .keyBy(WaterSensor::getVc)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 增量聚合函数 aggregate(aggFunction, windowFunction)
                //      1) aggFunction<IN, ACC, OUT>: 窗口增量聚合函数；
                //      2) windowFunction<IN, OUT, KEY, W>: 全窗口函数。接收aggFunction处理完整个窗口后的输出作为输入，进行后续处理
                .aggregate(new VcAggregateFunction(), new VcProcessWindowFunction());

        // 2、按照窗口标签keyBy，保证同一个窗口时间范围的结果到一个process方法中做处理：排序、取TopN
        windowAgg
                .keyBy(value -> value.f2)
                // 做TopN，没有现成算子，就用process()
                .process(new VcKeyedProcessFunction(2))
                .print();

        env.execute();
    }

    public static class VcAggregateFunction implements AggregateFunction<WaterSensor, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }
        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }
        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    /**
     * 输入：增量函数 VcAggregateFunction 的输出，Integer 类型，内容就是vc的计数值 count
     * 输出：Tuple3<Integer, Integer, Long> 类型，内容是 Tuple3(vc, count, context.window().getEnd()) 带上窗口结束时间标签
     */
    public static class VcProcessWindowFunction extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            // 输入数据的迭代器，里面只有一条数据，直接取出即可
            Integer count = elements.iterator().next();
            long end = context.window().getEnd();
            out.collect(Tuple3.of(key, count, end));
        }
    }

    // 以之前开窗的 windowEnd 做 keyBy 后的窗口处理函数
    public static class VcKeyedProcessFunction extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
        // 1、使用Map存储窗口中不同windowEnd所有vc的统计结果：key=windowEnd value=统计结果
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> vcMap;
        // Top的数量
        private int threshold;

        public VcKeyedProcessFunction(int threshold) {
            this.threshold = threshold;
            vcMap = new HashMap<>();  // HashMap 初始化
        }


        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 1）存入HashMap
            long windowEnd = value.f2;
            List<Tuple3<Integer, Integer, Long>> vcList = vcMap.get(windowEnd) == null? new ArrayList<>() : vcMap.get(windowEnd);
            vcList.add(value);
            vcMap.put(windowEnd, vcList);

            // 注册定时器，windowEnd+1即可
            // 同一个窗口范围，应该同时输出，只不过是一条一条调用，所以只需要延迟1ms就行
            TimerService timer = ctx.timerService();
            timer.registerEventTimeTimer(windowEnd+1);
        }

        /**
         * 触发窗口：同一个窗口的统计结果攒齐了
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 2）排序
            Long windowEnd = ctx.getCurrentKey();
            List<Tuple3<Integer, Integer, Long>> vcAgg = vcMap.get(windowEnd);

            Collections.sort(vcAgg, (o1, o2) -> o2.f1-o1.f1);

            // 3）取出TopN
            StringBuilder outStr = new StringBuilder();
            outStr.append("==============================\n");
            for (int i=0; i<Math.min(vcAgg.size(), threshold); i++) {
                outStr.append("TOP" + (i+1) +"的vc: ");
                outStr.append("vc="+vcAgg.get(i).f0);
                outStr.append(", count=").append(vcAgg.get(i).f1);
                outStr.append("窗口结束时间："+ DateFormatUtils.format(windowEnd, "yyyy-MM-dd HH:mm:ss.SSS\n"));
            }
            outStr.append("==============================\n");

            vcMap.clear();  // 清理资源，节省资源（不处理逻辑也没问题）
            out.collect(outStr.toString());
        }
    }
}