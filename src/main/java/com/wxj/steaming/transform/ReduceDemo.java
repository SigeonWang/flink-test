package com.wxj.steaming.transform;

import com.wxj.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 19:00
 * @Description: TODO
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 11),
                new WaterSensor("s1", 3L, 8),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // TODO reduce
        // 要点：
        //   1、keyBy之后才能调用
        //   2、输入类型 = 输出类型，不够灵活。
        //   3、每个key的第一条数据不会执行reduce方法，而是直接输出（同时会缓存起来）
        //   4、reduce方法两个参数：
        //        value1：之前的计算结果
        //        value2：现在来的数据
        KeyedStream<WaterSensor, String> ks = ds.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        SingleOutputStreamOperator<WaterSensor> res = ks.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("value1=" + value1);
                System.out.println("value2=" + value2);
                return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
            }
        });

        res.print();

        env.execute();
    }
}
