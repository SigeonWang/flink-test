package com.wxj.steaming.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/1 12:06
 * @Description: TODO
 */
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                Tuple2.of("a", 1),  // [-3, 3]
                Tuple2.of("a", 3),  // [-1, 5]
                Tuple2.of("a", 7),  // [3, 9]
                Tuple2.of("b", 1),  // [-3, 3]
                Tuple2.of("b", 6),  // [2, 8]
                Tuple2.of("c", 11)  // [7, 13]
        )
        .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // 注意forMonotonousTimestamps是泛型方法，这里需要指定泛型
                        .<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((r, ts) -> r.f1 * 1000)
        );
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                Tuple3.of("a", 1, 1),
                Tuple3.of("a", 9, 4),
                Tuple3.of("b", 1, 5),
                Tuple3.of("c", 5, 12),
                Tuple3.of("c", 11, 13)

        )
        .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((r, ts) -> r.f1 * 1000)
        );

        /**
         * TODO interval join：有序流
         *
         */
        SingleOutputStreamOperator<String> res = ds1.keyBy(r -> r.f0).intervalJoin(ds2.keyBy(r -> r.f0))
                .between(Time.seconds(-4), Time.seconds(2))
                // 注意：ProcessJoinFunction 是一个抽象类，不是函数式接口，不能转为Lambda表达式
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "<--->" + right);
                    }
                });

        res.print();

        env.execute();
    }
}
