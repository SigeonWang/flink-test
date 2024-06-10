package com.wxj.steaming.process;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/2 16:35
 * @Description: 对每个传感器，水位超过10的输出告警信息x`
 */
public class SideOutputDemo {
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

        OutputTag<String> monitorTag = new OutputTag<>("monitor", Types.STRING);
        SingleOutputStreamOperator<WaterSensor> res = ds.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        // 使用侧输出流告警
                        Integer vc = value.getVc();
                        String monitorStr = "当前水位 " + vc + ", 高于阈值！";
                        if (vc > 10) {
                            ctx.output(monitorTag, monitorStr);
                        }
                        // 主流正常发送数据
                        out.collect(value);
                    }

                });
        res.print("主流");
        res.getSideOutput(monitorTag).printToErr("告警流");

        env.execute();
    }
}
