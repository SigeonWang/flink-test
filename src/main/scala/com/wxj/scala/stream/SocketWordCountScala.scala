package com.wxj.scala.stream

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 *
 * 注意：
 *    DataStream API的环境上下文是StreamExecutionEnvironment、分组函数是keyBy();
 *    DataStream API 最后需要调用execute()方法触发执行.
 *
 */
object SocketWordCountScala {
  def main(args: Array[String]): Unit = {
    // TODO 1、创建环境

    val conf = new Configuration()  // 不添加配置项的话就都用默认的配置

    // 带web ui的本地测试环境，一般只用于测试。需要引入以来flink-runtime-web。
     val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    // 推荐做法，自动推测运行环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 全局并行度设置：算子 > env > 提交命令 > 配置文件
    //    算子关系：一对一；重分区
    //    注意：在开发环境中，由于没有flink配置文件，默认并行度就是当前机器的CPU核心数
    env.setParallelism(1)

    // 全局禁用算子链（一般不需要禁用，自动优化即可。特殊：算子复杂度较高时；为了定位具体算子问题时）
    //    算子链条件：一对一关系 + 并行度相同
    //    也可以针对某个算子进行禁用，其前后都不会加入算子链：flatMap().disableChaining()
    //    也可以针对某个算子开启后续的算子链，flatMap().startNewChain()
//    env.disableOperatorChaining()

    // 新版flink流批一体，默认流，可以设置DataSet还是DataStream，使用一套代码。
    // 一般通过命令提交时指定：-Dexecution.runtime-mode=Batch
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

    // TODO 2、读取数据源
    val linesDS = env.socketTextStream("localhost", 6666)

    // TODO 3、切分、转换、分组、聚合
    val wordsDS = linesDS.flatMap(_.split(" "))
    // 注意：一般不会在程序中设置全局并行度，因为如果在程序中对全局并行度进行硬编码，会导致无法动态扩容
    val tupleDS = wordsDS.map((_, 1)).setParallelism(2)
    // 注意：keyBy是个hash操作，不是算子，所以无法对keyBy设置并行度
    val tupleKS = tupleDS.keyBy(_._1)
    val resTupleDS = tupleKS.sum(1)

    // TODO 4、输出
    resTupleDS.print()

    // TODO 5、触发执行
    env.execute()
  }
}
