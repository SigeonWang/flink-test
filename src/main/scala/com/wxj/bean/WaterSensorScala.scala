package com.wxj.bean

import java.util.Objects
import scala.beans.BeanProperty

/**
 * Scala 数据类型
 *    基本：Byte, Short, Int, Long, Float, Double, Char, String, Boolean
 *    其他：Unit(实例()), Null(null 或 空引用), Nothing, Any, AnyRef
 *
 */
class WaterSensorScala {
  @BeanProperty var id: String = _
  @BeanProperty var ts: Long = _
  @BeanProperty var vc: Int = _

  override def toString: String = {
    // super.toString
    s"WaterSensor{id=$id, ts=$ts, vc=$vc}"
  }

  override def equals(obj: Any): Boolean = {
    // super.equals(obj)
    if (this == obj) {
      return true
    }
    if (obj == null || getClass != obj.getClass) {
      return false
    }
    val that = obj.asInstanceOf[WaterSensorScala]
    return Objects.equals(id, that.id) &&
      Objects.equals(ts, that.ts) &&
      Objects.equals(vc, that.vc)
  }

  override def hashCode(): Int = {
    super.hashCode()
  }
}
