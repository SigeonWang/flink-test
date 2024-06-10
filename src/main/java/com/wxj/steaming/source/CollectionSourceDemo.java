package com.wxj.steaming.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 14:44
 * @Description: TODO
 */
public class CollectionSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 方法一：从集合创建ds（一般只用于测试）
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);
//                .fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        source.print();

        env.execute();

    }
}
