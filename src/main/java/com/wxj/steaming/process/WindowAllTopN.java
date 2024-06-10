package com.wxj.steaming.process;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/2 12:15
 * @Description: 实时统计一段时间内的出现次数最多的水位。
 *     要求：统计最近10秒钟内出现次数最多的两个水位，并且每5秒钟更新一次。
 */
public class WindowAllTopN {
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
         * TODO 思路一： 使用不分流的窗口（并行度自动为1），将所有数据集合到一起， 用hashmap存储：key=vc，value=count值
         *
         * 特点：
         *      没有分流，并行度1，效率低；
         *      使用全窗口函数，先要收集到所有数据，不够高效。
         */
        ds.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        // 1、使用Map存储每个vc出现的次数
                        Map<Integer, Integer> cntMap = new HashMap<>();
                        for (WaterSensor e: elements) {
                            Integer vc = e.getVc();
                            int num = cntMap.get(vc) == null? 0: cntMap.get(vc);
                            cntMap.put(vc, num+1);
                        }
                        // 2、使用List对vc出现的次数排序
                        List<Map.Entry<Integer, Integer>> values = new ArrayList<>(cntMap.entrySet());
                        Collections.sort(values, (o1, o2) -> o2.getValue()-o1.getValue());

                        // 3、取出最高的两个vc
                        StringBuilder outStr = new StringBuilder();
                        outStr.append("==============================\n");
                        // 遍历排序后结果。注意边界条件：如果10s内数据没有2条，就输出集合左右的，防止数组越界
                        for (int i=0; i<Math.min(values.size(), 2); i++) {
                            outStr.append("TOP" + (i+1) +"的vc: ");
                            outStr.append("vc="+values.get(i).getKey());
                            outStr.append(", count=").append(values.get(i).getValue());
                            outStr.append("窗口结束时间："+ DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS\n"));
                        }
                        outStr.append("==============================\n");
                        out.collect(outStr.toString());
                    }
                })
                .print();

        env.execute();
    }
}
