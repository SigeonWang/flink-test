package com.wxj.steaming.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 16:33
 * @Description: TODO
 */
public class DataGenSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 方法二：从数据生成器创建ds
        // 在新版source架构下，如果需要从外部系统导入数据，那么需要添加连接器导入相关依赖 flink-connector-datagen
        // flink 1.11 开始提供数据生成器，用于任务及性能测试，1.17提供了新的source写法。
        DataGeneratorSource<String> dataGenSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number" + value;
                    }
                },
                10,  // 数据上限（如果并行度n，则每个并行任务上生成数值量是10/n）
                RateLimiterStrategy.perSecond(1),  // 数据生成速率
                Types.STRING()
        );

        DataStreamSource<String> ds = env.fromSource(dataGenSource, WatermarkStrategy.noWatermarks(), "dateGen-source");
//                .readTextFile("")  // 底层调用的addSource()方法，已经过期

        ds.print();

        env.execute();

    }
}
