package com.wxj.steaming.shunt;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 21:58
 * @Description: 分流：watersensor 数据，分开s1, s2
 *
 * 应用：侧输出流可以在不影响主流数据处理的前提下，进行数据的告警，或者处理延迟数据
 * 步骤：
 *    1、使用process算子
 *    2、定义OutputTag
 *    3、调用ctx.output
 *    4、通过主流获取侧流数据
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 6666)
                .map(new WaterSensorMapFunction());

        // TODO 使用 侧输出流 实现
        // 当现有DataStream的算子不能满足需求时，可以调用底层process()算子，更灵活的自定义逻辑
        // new ProcessFunction<WaterSensor, Object>() {}
        //      1、WaterSensor 输入数据类型
        //      2、Object      输出数据类型（主流）
        // public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out)
        //      1、value 输入元素
        //      2、ctx   上下文
        //      3、out   输出（采集器）
        // 创建outputTag对象:
        //     参数1:标签名
        //     参数2: 放入侧输出流中的数据的类型，需要是 TypeInformation
        OutputTag<WaterSensor> tag1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> tag2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> processDs = ds.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.getId();
                if ("s1".equals(id)) {  // id=="s1" 数据放入侧输出流s1
                    ctx.output(tag1, value);
                } else if ("s2".equals(id)) {  // id=="s2" 数据放入侧输出流s2
                    ctx.output(tag2, value);
                } else {
                    // 其他，还放在主流即可
                    out.collect(value);
                }
            }
        });

        // 打印主流
        processDs.print("主流");
        // 打印侧输出流
        processDs.getSideOutput(tag1).printToErr("s1流");  // 输出到错误日志
        processDs.getSideOutput(tag2).print("s1流");

        env.execute();
    }
}
