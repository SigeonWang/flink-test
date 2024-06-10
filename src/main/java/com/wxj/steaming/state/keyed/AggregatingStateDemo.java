package com.wxj.steaming.state.keyed;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/3 22:14
 * @Description: 统计每种传感器的平均水位。
 */
public class AggregatingStateDemo {
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

        // 键控状态：对每个子任务的key单独存储状态
        ds.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // TODO 1、声明状态
                    //      IN: vc
                    //      OUT：avg
                    AggregatingState<Integer, Double> vcAggregatingState;

                    /**
                     * TODO 2、状态更新：open()
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 2.1 获取状态
                        RuntimeContext context = getRuntimeContext();
                        // getAggregatingState(): 获取规约状态
                        //      IN：vc值
                        //      ACC：累加值，Tuple2.of(sum, count)
                        //      OUT：avg = sum / count
                        vcAggregatingState = context.getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>("vcAggregatingState",
                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    // 初始化acc
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0, 0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                        return Tuple2.of(accumulator.f0+value, accumulator.f1+1);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        return accumulator.f0 * 1.0 / accumulator.f1;
                                    }

                                    // merge(): 合并两个累加器 (可不重写)
                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
//                                        return Tuple2.of(a.f0+b.f0, a.f1+b.f1);
                                        return null;
                                    }
                                }
                                , Types.TUPLE(Types.INT, Types.INT)));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 把每条数据的vc添加到reducing状态里
                        vcAggregatingState.add(value.getVc());
                        // 获取当前的reducing状态值
                        Double vcSum = vcAggregatingState.get();
                        out.collect("INFO：传感器"+value.getId()+" => 所有水位的平均值 "+vcSum);
                    }
                }).print();

        env.execute();
    }
}
