package com.wxj.function;

import com.wxj.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: xingjian wang
 * @Date: 2024/5/29 17:25
 * @Description: TODO
 */
public class WaterSensorFilterFunction implements FilterFunction<WaterSensor> {

    public String id;

    public WaterSensorFilterFunction(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return this.id.equals(value.getId());
    }
}
