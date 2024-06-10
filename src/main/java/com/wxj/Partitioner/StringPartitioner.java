package com.wxj.Partitioner;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.Map;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 21:35
 * @Description: TODO
 */
public class StringPartitioner implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
        return key.hashCode() % numPartitions;
    }
}
