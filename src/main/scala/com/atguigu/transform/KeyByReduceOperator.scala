package com.atguigu.transform

import com.atguigu.SensorEntity
import org.apache.flink.streaming.api.scala._

object KeyByReduceOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444),
      SensorEntity("s01", 1547719199, 35.80018327300259),
      SensorEntity("s02", 1547719201, 15.402984393403084),
      SensorEntity("s04", 1547719205, 38.101067604893444)
    ))
      .keyBy("id")
      // 输出当前传感器温度+10，时间戳是上一次数据时间+1
      .reduce((x, y) => SensorEntity(x.id, x.timestamp+1, y.temperature+10))

    sensorStream.print()

    env.execute("KeyByReduce Operator")
  }
}
