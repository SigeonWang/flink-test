package com.wxj.steaming.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/30 10:49
 * @Description: TODO
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 必须开启ck，否则文件不会关闭。
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);  // 设置2s一次ck，使用"精确一次"语义

        DataGeneratorSource<String> dataGenSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number" + value;
                    }
                },
                Long.MAX_VALUE,  // 数据上限（如果并行度n，则每个并行任务上生成数值量是10/n）
                RateLimiterStrategy.perSecond(10),  // 数据生成速率
                Types.STRING()
        );
        DataStreamSource<String> ds = env.fromSource(dataGenSource, WatermarkStrategy.noWatermarks(), "dateGen-source");

        // TODO 输出到文件系统
        // Flink1.12以前，sink算子的创建是通过调用stream.addSink()方法实现的；Flink1.12开始，同样重构了Sink架构，stream.sinkTo(…)
        // SimpleStringEncoder()：
        //      默认编码 UTF-8；
        //      也可以使用带参的构造方法设置编码
        // 注意：forRowFormat()是个泛型方法，需要指定类型！
        FileSink<String> rowFileSink = FileSink.<String>forRowFormat(new Path("src/main/resources/out"), new SimpleStringEncoder<>())
                // 文件名前缀，默认"part"; 后缀，默认""
                .withOutputFileConfig(new OutputFileConfig("file-out", ".log"))
                // 文件分桶规则（划分目录，每个目录都会生成"并行度"数量的文件）。默认 DateTimeBucketAssigner，默认使用系统时区，也可以指定。
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH", ZoneId.of("Asia/Shanghai")))
                // 文件滚动策略。默认 DefaultRollingPolicy，当达到128M / 连续写入60s / 间隔60s没有写入
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(new MemorySize(1024 * 1024))  // 1M
                                .withRolloverInterval(Duration.ofSeconds(10))
//                                .withInactivityInterval(Duration.ofSeconds(6))
                                .build()
                )
                .build();
        ds.sinkTo(rowFileSink);

        env.execute();
    }
}
