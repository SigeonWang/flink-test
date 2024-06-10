package com.wxj.steaming.partition;

import com.wxj.Partitioner.StringPartitioner;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 21:01
 * @Description: TODO
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> ds = env.socketTextStream("localhost", 6666);

        // TODO 1、shuffle: 随机分区。
        // ShufflePartitioner：核心 random.nextInt(numberOfChannels)
        // 注意：分区器的核心逻辑都在selectChannel()方法。
//        ds.shuffle().print();

        // TODO 2、rebalance: 轮询。
        // RebalancePartitioner：核心逻辑 nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels
//        ds.rebalance().print();

        // TODO 3、rescale: 缩放。
        // RescalePartitioner：实现轮训，局部组队，比rebalance更高效
//        ds.rescale().print();

        // TODO 4、broadcast: 广播
        // BroadcastPartitioner：发送给下游所有子任务
//        ds.broadcast().print();

        // TODO 5、global: 全局
        // GlobalPartitioner：只发送给第一个子任务。核心逻辑 return 0
//        ds.global().print();

        // TODO 6、one-to-one: 一对一
        // ForwardPartitioner

        // TODO 7、keyBy: 按key分组
        // KeyGroupStreamPartitioner：相同key发往同一个分区（子任务）

        // TODO 8、自定义
        // CustomPartitionerWrapper
        // partitionCustom() 两个参数:
        //     1、实现Partitioner接口，定义根据key进行分区的逻辑。
        //     2、实现KeySelector接口，定义获取key的逻辑。
        ds.partitionCustom(
            new StringPartitioner(),
            new KeySelector<String, String>() {
                @Override
                public String getKey(String value) throws Exception {
                    return value;
                }
            }
        ).print();

        env.execute();
    }
}
