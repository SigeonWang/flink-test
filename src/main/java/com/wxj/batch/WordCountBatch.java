package com.wxj.batch;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 *
 * 由于Java中泛型擦除的存在，在某些特殊情况下（比如Lambda表达式中），自动提取的信息是不够精细，这时就需要显式地提供类型信息！！！
 * 如果不想碰到这种问题，就使用匿名内部类或者自定义实现类避免
 *
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        // 1、创建环境
        // 注意：flink 新版主推批流一体的架构，不在推荐使用ExecutionEnvironment，而是统一使用StreamExecutionEnvironment！
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2、读取数据源
        DataSource<String> linesDS = env.readTextFile("src/main/resources/word.txt");

        // 3、切分、转换、分组、聚合
        FlatMapOperator<String, String> wordsRes = linesDS.flatMap(
                (String line, Collector<String> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
        ).returns(Types.STRING);  // 通过Types指定返回类型（推荐）
        MapOperator<String, Tuple2<String, Long>> mapRes = wordsRes.map(word -> Tuple2.of(word, 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {});  // 通过TypeHint指定
//                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        UnsortedGrouping<Tuple2<String, Long>> groupRes = mapRes.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sumRes = groupRes.sum(1);

        // 4、输出
        sumRes.print();
    }
}
