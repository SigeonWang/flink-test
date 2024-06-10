package com.wxj.bean;

import java.util.Objects;

/**
 * Flink POJO类型：
 *  1）类是公有（public）的
 *  2）有一个无参的构造方法
 *  3）所有属性都是公有（public）的（属性可访问就行，即变量可以是private，但是得用对应的get/set方法）
 *  4）所有属性的类型都是可以序列化的
 *
 * WaterSensor: 水位传感器
 *  id: 传感器类型
 *  ts: 记录时间戳
 *  vc: 水位
 */
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;

    public WaterSensor() {}

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(id, that.id) && Objects.equals(ts, that.ts) && Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, vc);
    }

}
