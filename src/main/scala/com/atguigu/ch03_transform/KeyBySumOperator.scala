package com.atguigu.ch03_transform

import com.atguigu.SensorEntity
import org.apache.flink.streaming.api.scala._

object KeyBySumOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置多余1的并行度，因为多线程的原因，结果不是按顺序执行
    // env.setParallelism(1)

    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444),
      SensorEntity("s01", 1547719199, 35.80018327300259),
      SensorEntity("s02", 1547719201, 15.402984393403084),
      SensorEntity("s04", 1547719205, 38.101067604893444)
    ))
//      .keyBy(0)
//      .sum(2)
        .keyBy("id")
        .sum("temperature")

    sensorStream.print()

    env.execute("KeyBySum Operator")
  }
}
