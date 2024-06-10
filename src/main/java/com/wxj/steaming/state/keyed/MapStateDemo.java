package com.wxj.steaming.state.keyed;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/3 21:44
 * @Description: 统计每种传感器的每种水位值出现的次数。
 */
public class MapStateDemo {
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
                    MapState<Integer, Integer> vcMapState;

                    /**
                     * TODO 2、状态更新：open()
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 2.1 获取状态
                        RuntimeContext context = getRuntimeContext();
                        // getMapState(): 获取Map状态
                        vcMapState = context.getMapState(new MapStateDescriptor<Integer, Integer>("vcMapState", Types.INT, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 1、把当前数据的vc存到list
                        Integer vcKey = value.getVc();
                        Integer vcCount = vcMapState.contains(vcKey)? vcMapState.get(vcKey) + 1: 1;
                        vcMapState.put(vcKey, vcCount);

                        Map<Integer, Integer> vcMap = new HashMap<>();
                        for (Map.Entry<Integer, Integer> entry : vcMapState.entries()) {
                            vcMap.put(entry.getKey(), entry.getValue());
                        }
                        out.collect("INFO：传感器"+value.getId()+" => 各水位出现的次数 "+vcMap.toString());
                    }
                }).print();

        env.execute();
    }
}
