package com.wxj.scala.stream

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SourceDemoScala {
  def main(args: Array[String]): Unit = {
    // TODO 1、创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // TODO 2、读取数据源
    val ds = env.socketTextStream("localhost", 6666)

    // TODO 3、切分、转换、分组、聚合
    val res = ds.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).sum(1)

    // TODO 4、输出
    res.print()

    // TODO 5、触发执行
    env.execute()
  }
}
