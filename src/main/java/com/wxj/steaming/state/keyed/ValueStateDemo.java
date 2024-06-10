package com.wxj.steaming.state.keyed;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/2 23:10
 * @Description: 检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警。
 */
public class ValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 在代码中指定状态后端
        // 1）使用HashMap（默认）
        // 优点：读写快，存在 TM 的 JVM堆内存 中；缺点是存不多(受限与TaskManager的内存)
        env.setStateBackend(new HashMapStateBackend());
        // 2）使用RocksDB
        // 存在 TM所在节点的rocksdb数据库，存到盘中，读写相对慢一些（写需要序列化，读需要反序列代），但是可以存很大的状态
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());

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
                    // process() 方法的局部变量的作用范围是每个子任务，其下不同key的区分ValueState会自动处理
                    // 如果不使用状态，自己实现可以通过 HashMap 区分key来存储，但效率较低
                    ValueState<Integer> lastVcState;

                    /**
                     * TODO 2、状态更新：open()
                     *
                     * 注意：
                     *      在open()外面是获取不到 RuntimeContext，跟类的加载顺序有关。
                     *      所以必须在open()里获取状态，即进行状态参数lastVcState的赋值。
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 2.1 获取状态
                        //      1）获取上下文
                        //      2）获取状态。传入状态描述器 ValueStateDescriptor，两个参数：状态名字；状态数据类型
                        RuntimeContext context = getRuntimeContext();
                        // getState(): 获取值状态
                        // TODO 状态TTL
                        //  1）创建 StateTtlConfig
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // TTL更新策略
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 过期状态是否可见
                                .build();
                        // TODO 状态TTL
                        //  2）创建状态描述起，启动 TTL
                        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("last-vc", Types.INT);
                        valueStateDescriptor.enableTimeToLive(ttlConfig);

                        lastVcState = context.getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 1、取出上一条数据的水位值
                        // value(): 获取状态的值，如果没有设置则返回null
                        Integer lastVc = lastVcState.value() == null? 0: lastVcState.value();

                        // 2、判断连续的两个水位值是否超过10
                        Integer curVc = value.getVc();
                        if (Math.abs(curVc - lastVc) > 10) {
                            out.collect("告警：传感器"+value.getId()+" => 当前水位值="+curVc+"与上条水位值="+lastVc+"相差超过10！");
                        }

                        // 3、更新状态的水位值
                        lastVcState.update(curVc);
                    }
                }).print();

        env.execute();
    }
}
