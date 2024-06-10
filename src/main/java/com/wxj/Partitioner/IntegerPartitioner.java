package com.wxj.Partitioner;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/31 18:19
 * @Description: TODO
 */
public class IntegerPartitioner  implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
