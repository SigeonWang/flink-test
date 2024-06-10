package com.wxj.steaming.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 14:44
 * @Description: TODO
 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 方法二：从文件创建ds
        // flink1.12以前，使用addSource(SinkFunction)方法；flink1.12以后的新版source架构使用fromSource()方法。
        // 如果需要从外部系统导入数据，直接添加连接器即可。导入相关依赖：flink-connector-files
        // 注意：这里是构造者模式，通过build()方法创建对象
        FileSource.FileSourceBuilder<String> fileSourceBuilder = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("src/main/resources/word.txt")
        );
        FileSource<String> fileSource = fileSourceBuilder.build();

        DataStreamSource<String> ds = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");
//                .readTextFile("")  // 底层调用的addSource()方法，已经过期

        ds.print();

        env.execute();

    }
}
