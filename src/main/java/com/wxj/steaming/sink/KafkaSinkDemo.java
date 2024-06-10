package com.wxj.steaming.sink;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/30 12:24
 * @Description: TODO
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // TODO 【必须】1、开启ck，否则在kafka生产者EXACTLY_ONCE语义下无法发送消息！
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<String> ds = env.socketTextStream("localhost", 6666);

        /**
         * TODO 输出到 Kafka
         *
         * 如果使用 EXACTLY_ONCE 写kafka，需要满足以下条件：
         *    1）开启ck
         *    2）设置事务ID的前缀
         *    3）设置事务超时时间（ck间隔 < 超时时间 < max 15min）
         *
         * 如果是 AT_LEAST_ONCE，以上三点无需满足！
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop01:9092,hadoop02:9092,hadoop03:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                            .setTopic("topic_1")
                            .setValueSerializationSchema(new SimpleStringSchema())  // 只序列化value，忽略key
                            .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // TODO 【必须】2、设置事务ID的前缀，否则在kafka生产者EXACTLY_ONCE语义下无法发送消息！
                .setTransactionalIdPrefix("flink-")
                // TODO 【必须】3、设置事务超时时间，否则在kafka生产者EXACTLY_ONCE语义下无法发送消息！
                // 超时时间限制：大于ck间隔，小于max(15min)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(10*60*1000))
                .build();

        ds.sinkTo(kafkaSink);

        env.execute();
    }
}
