package com.wxj.batch

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment


/**
 *
 * 注意：
 *    DataSet API的环境上下文是ExecutionEnvironment、分组函数是groupBy();
 *    flink1.12开始DataSet API已被逐渐弃用，官方建议使用Table API / SQL执行批处理，或者使用带有批处理的DataStream API.
 *
 */
object WordCountScala {
  def main(args: Array[String]): Unit = {
    // 1、创建环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2、读取数据源
    val linesDS = env.readTextFile("src/main/resources/word.txt")

    // 3、切分、转换、分组、聚合
    val wordsDS = linesDS.flatMap(_.split(" "))
    val tupleDS = wordsDS.map((_, 1))
    val tupleKS = tupleDS.groupBy(0)
    val resTupleDS = tupleKS.sum(1)

    // 4、输出
    resTupleDS.print()
  }
}
