package com.wxj.steaming.state.keyed;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/3 22:00
 * @Description: 统计每种传感器的水位和。
 */
public class ReducingStateDemo {
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
                    ReducingState<Integer> vcReducingState;

                    /**
                     * TODO 2、状态更新：open()
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 2.1 获取状态
                        RuntimeContext context = getRuntimeContext();
                        // getReducingState(): 获取规约状态
                        vcReducingState = context.getReducingState(new ReducingStateDescriptor<Integer>("vcReducingState", Integer::sum, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 把每条数据的vc添加到reducing状态里
                        vcReducingState.add(value.getVc());
                        // 获取当前的reducing状态值
                        Integer vcSum = vcReducingState.get();
                        out.collect("INFO：传感器"+value.getId()+" => 所有水位的和 "+vcSum);
                    }
                }).print();

        env.execute();
    }
}
