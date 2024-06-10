package com.wxj.steaming.transform;

import com.wxj.bean.WaterSensor;
import com.wxj.function.WaterSensorFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 17:10
 * @Description: TODO
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // TODO filter: true 保留；false 忽略
        SingleOutputStreamOperator<WaterSensor> res = ds.filter(new WaterSensorFilterFunction("s1"));
//                .filter(sensor -> "s1".equals(sensor.getId()));

        res.print();

        env.execute();
    }
}
