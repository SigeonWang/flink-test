package com.wxj.steaming.transform;

import com.wxj.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 17:10
 * @Description: TODO 如果是s1 只打印vc； 如果是s2 打印ts + vc
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        /**
         * flatMap: 一进多出
         *    对于s1 一进一出
         *    对于s2 一进二出
         *    对于s3 一进零出
         */
        SingleOutputStreamOperator<String> res = ds.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    // 如果s1，输出vc
                    out.collect(value.getVc().toString());
                } else if ("s2".equals(value.getId())) {
                    // 如果s1，输出ts + vc
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            }
        });

        res.print();

        env.execute();
    }
}
