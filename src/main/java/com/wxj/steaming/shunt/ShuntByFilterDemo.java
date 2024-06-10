package com.wxj.steaming.shunt;

import com.wxj.Partitioner.StringPartitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 21:01
 * @Description: 分流：奇数、偶数拆分成不同的流
 */
public class ShuntByFilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> ds = env.socketTextStream("localhost", 6666);

        // TODO 使用filter实现
        // 缺点：同一个数据，要被处理两遍，效率较低（调用两次filter）
        SingleOutputStreamOperator<String> stream1 = ds.filter(value -> (Integer.parseInt(value) % 2) == 0);
        stream1.print("偶数流");

        SingleOutputStreamOperator<String> stream2 = ds.filter(value -> (Integer.parseInt(value) % 2) == 1);
        stream2.print("奇数流");

        env.execute();
    }
}
