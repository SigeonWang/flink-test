package com.wxj.steaming.join;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * @Date: 2024/6/1 12:06
 * @Description: TODO
 */
public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("a", 7),
                Tuple2.of("b", 1),
                Tuple2.of("b", 6),
                Tuple2.of("c", 11)
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
         * TODO window join
         * join() 实现类似inner join的效果
         *
         *
         */
        DataStream<String> res = ds1.join(ds2)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .apply((v1, v2) -> v1+"<--->"+v2)
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                        return first + "<--->" + second;
                    }
                });
//
        res.print();

        env.execute();
    }
}
