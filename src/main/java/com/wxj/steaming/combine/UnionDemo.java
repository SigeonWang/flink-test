package com.wxj.steaming.combine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 22:35
 * @Description: 合流
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> ds2 = env.fromElements(11, 22, 33, 44, 55);
        DataStreamSource<String> ds3 = env.fromElements("-1", "-2", "-3", "-4", "-5");

        // TODO 合流 union()
        // 要点：
        //     1、合并的流数据类型要一致（不够灵活）
        //     2、一次可以合并多条流
//        DataStream<Integer> unionDs = ds1.union(ds2).union(ds3.map(v -> Integer.valueOf(v)));
        DataStream<Integer> unionDs = ds1.union(ds2, ds3.map(v -> Integer.valueOf(v)));
        unionDs.print();

        env.execute();
    }
}
