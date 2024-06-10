package com.wxj.steaming.state.keyed;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * @Date: 2024/6/3 21:13
 * @Description: 检测每种传感器，输出最高的3个水位值。
 */
public class ListStateDemo {
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
                    ListState<Integer> vcListState;

                    /**
                     * TODO 2、状态更新：open()
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 2.1 获取状态
                        RuntimeContext context = getRuntimeContext();
                        // getListState(): 获取列表状态
                        vcListState = context.getListState(new ListStateDescriptor<Integer>("vcListState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 1、把当前数据的vc存到list
                        vcListState.add(value.getVc());
//                        vcListState.addAll(Arrays.asList(1,2));  // 一次添加多条

                        // 2、取出list，排序，并取前3
                        Iterable<Integer> vcListIt = vcListState.get();
                        // 由于get()返回的是一个可迭代类型，需要拷贝到一个List后再排序
                        List<Integer> vcList = new ArrayList<>();
                        for (Integer vc: vcListIt) {
                            vcList.add(vc);
                        }
                        vcList.sort(Comparator.reverseOrder());
                        if (vcList.size()>3) {
                            // 注意数据一条一条处理，这里只需要每次判断是不是大于3了，大于就删除第4个就行了
                            vcList.remove(3);
                        }
                        out.collect("INFO：传感器"+value.getId()+" => 前3的vc值 "+vcList.toString());

                        // 3、更新list
                        vcListState.update(vcList);
                    }
                }).print();

        env.execute();
    }
}
