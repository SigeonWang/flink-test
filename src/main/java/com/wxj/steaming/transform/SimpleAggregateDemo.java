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
public class SimpleAggregateDemo {
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

        // TODO 简单聚合算子
        // 要点：
        //   1、keyBy之后才能调用
        //   2、传位置索引的算子，Tuple类型才适用，POJO不可以（可以传变量名）。
        //   3、max 取比较字段的最大值，非比较字段保留第一次的值；maxBy 取比较字段的最大值，非比较字段也取该条数据的值。min/minBy类似
        KeyedStream<WaterSensor, String> ks = ds.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
//        SingleOutputStreamOperator<WaterSensor> res = ks.sum("vc");
//        SingleOutputStreamOperator<WaterSensor> res = ks.min("vc");
//        SingleOutputStreamOperator<WaterSensor> res = ks.minBy("vc");
//        SingleOutputStreamOperator<WaterSensor> res = ks.max("vc");
        SingleOutputStreamOperator<WaterSensor> res = ks.maxBy("vc");

        res.print();

        env.execute();
    }
}

