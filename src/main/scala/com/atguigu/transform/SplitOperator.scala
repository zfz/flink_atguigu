package com.atguigu.transform

import com.atguigu.SensorEntity
import org.apache.flink.streaming.api.scala._

object SplitOperator {
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

    val splitStream: SplitStream[SensorEntity] = sensorStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })

    val high: DataStream[SensorEntity] = splitStream.select("high")
    val low: DataStream[SensorEntity] = splitStream.select("low")
    val all: DataStream[SensorEntity] = splitStream.select("high", "low")

    high.print("high")
    low.print("low")
    all.print("all")

    env.execute("Split Operator")
  }
}
