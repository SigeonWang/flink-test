package com.wxj.function;

import com.wxj.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 17:25
 * @Description: TODO
 */
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        String[] data = value.split(",");
        // parseXxx() 包装类 -> 基本数据类型
        // valueOf()  基本数据类型 -> 包装类
        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
    }
}
