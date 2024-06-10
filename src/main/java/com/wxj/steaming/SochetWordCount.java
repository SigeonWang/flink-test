package com.wxj.steaming;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class SochetWordCount {
    public static void main(String[] args) throws Exception {
        // 1、创建环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);  // 默认模式：流

        // 2、读取数据源
        // TODO 方法三：从socket创建ds（一般只用于测试）
        DataStreamSource<String> source = env.socketTextStream("localhost", 6666);

        // 3、切分、转换、分组、聚合
        SingleOutputStreamOperator<String> wordsRes = source.flatMap(
                (String line, Collector<String> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
        ).returns(Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Long>> mapRes = wordsRes.map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> keyRes = mapRes.keyBy(value -> value.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sumRes = keyRes.sum(1);

        // 4、输出
        sumRes.print();

        // 5、触发执行
        env.execute("flink-test-job");
        // 特殊：异步执行，可以在同一个main中启动多个job，一般不用
        // env.executeAsync();
    }
}
