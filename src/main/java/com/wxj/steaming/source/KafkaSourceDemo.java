package com.wxj.steaming.source;

import com.wxj.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;


/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 14:48
 * @Description: TODO
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 方法四：从Kafka创建ds
        // 在新版source架构下，如果需要从外部系统导入数据，那么需要添加连接器导入相关依赖 flink-connector-kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()  // builder()是泛型方法，需要指定类型
                .setBootstrapServers("hadoop01:9092,hadoop02:9092,hadoop03:9092")  // kafka节点地址和端口
                .setGroupId("flink-test-consumer")  // kafka消费者组
                .setTopics("topic_1")  // kafka主题
                .setValueOnlyDeserializer(new SimpleStringSchema())  // 反序列化类型（只针对value，key忽略）
                .setStartingOffsets(OffsetsInitializer.latest())  // flink消费kafka策略。setStartingOffsets() 默认earliest
//                .setProperty();  // 通用的参数设置方法，一般常用的已经封装好方法，直接使用即可。
                .build();

        // 不使用水位线
//        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        /**
         * TODO 使用水位线：在 source 处生成
         *
         * 注意：如果在source处生成watermark，就不用再调用assignTimestampsAndWatermarks()了
         *
         */
        DataStreamSource<String> ds = env.fromSource(
                kafkaSource,
                WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofSeconds(10))
//                        .withTimestampAssigner()  // 连接器已经内置了提取事件时间的逻辑，不用再调用时间戳生成器！！！
                ,
                "kafka-source"
        );


        ds.print();

        env.execute();

    }
}
