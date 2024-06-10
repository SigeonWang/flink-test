package com.wxj.steaming.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;


/**
 * @Author: xingjian wang
 * @Date: 2024/6/10 18:02
 * @Description: TODO
 */
public class KafkaEosDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 1、启用检查点，并设置精确一次
        env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop01:8020/chk");
        System.setProperty("HADOOP_USER_NAME", "root");
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // TODO 2、读取Kafka
        // 在新版source架构下，如果需要从外部系统导入数据，那么需要添加连接器导入相关依赖 flink-connector-kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()  // builder()是泛型方法，需要指定类型
                .setBootstrapServers("hadoop01:9092,hadoop02:9092,hadoop03:9092")
                .setGroupId("flink-test-consumer")
                .setTopics("topic_1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
//                .setProperty();  // 通用的参数设置方法，一般常用的已经封装好方法，直接使用即可。
                .build();

        DataStreamSource<String> kafkaDs = env.fromSource(
                kafkaSource,
                WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofSeconds(10))
//                        .withTimestampAssigner()  // 连接器已经内置了提取事件时间的逻辑，不用再调用时间戳生成器！！！
                ,
                "kafka-source"
        );


        // TODO 处理逻辑
        // kafkaDs.map()
        // ...


        // TODO 3、写出到Kafka
        // 在 EXACTLY_ONCE语义下写入Kafka，需满足以下条件：
        //     1）开启chk
        //     2）设置事务前缀
        //     3）设置事务超时时间：chk间隔（默认0ms） < kafka事务超时时间（默认1min）< kafka事务最大超时时间(15min)
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop01:9092,hadoop02:9092,hadoop03:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("topic_1")
                                .setValueSerializationSchema(new SimpleStringSchema())  // 只序列化value，忽略key
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //【必须】设置事务ID的前缀
                .setTransactionalIdPrefix("flink-")
                //【必须】设置事务超时时间（默认1min）
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(10*60*1000))
                .build();

        kafkaDs.sinkTo(kafkaSink);

        env.execute();

    }
}
