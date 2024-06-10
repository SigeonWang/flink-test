package com.wxj.steaming.checkpoint;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: xingjian wang
 * @Date: 2024/6/10 14:21
 * @Description:
 */
public class CheckpointDemo {
    public static void main(String[] args) throws Exception {

        // 开启最终检查点（flink 1.15 开始，默认开启，一般不建议修改）
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        // TODO 检查点配置
        // 1、启动chk: 周期10s，精确一次
        env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE);
        // 获取chk配置对象
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2、chk检查点保存位置（默认只保存最近一次执行成功的chk信息）
        // 使用hdfs路径，本地测试时需要引入hadoop-client依赖，并且指定访问hdfs的用户名避免权限问题
//        checkpointConfig.setCheckpointStorage("hdfs://hadoop01:8020/chk");
//        System.setProperty("HADOOP_USER_NAME", "root");
        // 使用本地文件路径，测试用
        checkpointConfig.setCheckpointStorage("src/main/resources/chk");
        // 3、chk超时时间(默认10min，一般不用修改)
        checkpointConfig.setCheckpointTimeout(600 * 1000);
        // 4、chk同时开启的最大数量（默认1）
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        // 5、两次chk的最小等待间隔（默认0ms，如果设置大于0，chk并发数就会强制置为1）
        checkpointConfig.setMinPauseBetweenCheckpoints(5 * 1000);
        // 6、取消作业时是否保存chk到外部系统（默认NO_EXTERNALIZED_CHECKPOINTS，建议设置为 RETAIN_ON_CANCELLATION）
        // 注意，这里的配置是程序正常取消的情况下，如果程序异常挂掉，DELETE_ON_CANCELLATION 也删除不了最近的chk
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 7、chk容忍失败次数（默认0，表示chk失败即任务失败挂掉，一般需要设置大于0来容忍失败）
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        // 8、开启非对齐检查点（默认不开启，开启的前提：检查点语义 Exactly Once，且并发 1）
        checkpointConfig.enableUnalignedCheckpoints();
        // 9、（开启非对齐检查点后生效）对齐检查点超时时间：
        // 默认0：表示使用非对齐检查点
        // 大于0：优先使用非对齐检查点，当对齐时间超过此参数，则使用非对齐检查点
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofMillis(15));

        // 10、开启changelog（默认不开启，开启的前提：并发 1，其他参数建议在配置文件指定）
        // 本地测试时需要引入依赖 flink-statebackend-changelog
        // 存储类型：filesystem（建议） / memory。实验功能，暂时不建议上生产，开启会增加资源消耗。
//        env.enableChangelogStateBackend(true);


        env.socketTextStream("localhost", 6666)
                .flatMap((String line, Collector<String> out) -> {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                )
                .map(word -> Tuple2.of(word, 1L)).uid("map-wc")  // savepoint 算子id设置（给系统区分算子用）
                .keyBy(value -> value.f0)
                .sum(1).uid("sum-wc").name("sum_1")  // 指定算子别名（给自己区分算子用，会显示到webui，方便查找问题）
                .print();

        env.execute();
    }
}
