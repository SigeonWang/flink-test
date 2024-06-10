package com.wxj.function;

import com.wxj.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 17:25
 * @Description: TODO
 */
public class StringMapFunction implements MapFunction<WaterSensor, String> {

    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
