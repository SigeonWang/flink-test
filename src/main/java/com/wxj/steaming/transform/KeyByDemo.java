package com.wxj.steaming.transform;

import com.wxj.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 18:09
 * @Description: TODO
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // TODO keyBy 按照id进行分组：arg1 in；arg2 key
        // 对于Flink而言，DataStream是没有直接进行聚合的API的。在Flink中，要做聚合需要先进行分区（提高效率），这个操作是通过keyBy来完成的。
        // 要点：
        //   1、返回的是一个KeyedStream，键控流
        //   2、keyBy 不是转换算子，只是对数据重分区，不能设置并行度
        //   3、分区和分组的关系
        //      1）keyBy是对数据分组，相同key的数据一定在同一分区
        //      2）分区：是并行度的概念，一个子任务可以理解为一个分区，一个分区内可以存在多个分组
        KeyedStream<WaterSensor, String> res = ds.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        res.print();

        env.execute();
    }
}

