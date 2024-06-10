package com.wxj.steaming.transform;

import com.wxj.bean.WaterSensor;
import com.wxj.function.StringMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 17:10
 * @Description: TODO
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // TODO 方法一：匿名内部类
        ds.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        // TODO 方法二：lambda表达式
        ds.map(sensor -> sensor.getId());
        // ds.map(WaterSensor::getId);

        // TODO 方法三：定义一个类实现MapFunction（生产中常用。抽象出统一的转换逻辑，多个任务中可以共用）
        SingleOutputStreamOperator<String> res = ds.map(new StringMapFunction());

        res.print();

        env.execute();
    }

    // 可以在同一个文件定义静态类，但不规范，一般放到外面一个单独的包
//    public static class MyMapFunction implements MapFunction<WaterSensor, String> {
//        @Override
//        public String map(WaterSensor value) throws Exception {
//            return value.getId();
//        }
//    }
}
