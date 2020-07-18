package com.atguigu.ch03_transform

import com.atguigu.SensorEntity
import org.apache.flink.streaming.api.scala._

object ConnectOperator {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444),
      SensorEntity("s01", 1547719199, 35.80018327300259),
      SensorEntity("s02", 1547719201, 15.402984393403084),
      SensorEntity("s04", 1547719205, 38.101067604893444)
    ))

    val splitStream: SplitStream[SensorEntity] = sensorStream.split(
      data => if (data.temperature > 30) Seq("high") else Seq("low")
    )

    val high: DataStream[SensorEntity] = splitStream.select("high")
    val low: DataStream[SensorEntity] = splitStream.select("low")

    val warning: DataStream[(String, Double)] = high.map(data => (data.id, data.temperature))

    val connectedStream: ConnectedStreams[(String, Double), SensorEntity] = warning.connect(low)

    // 两条流数据结构可以不同，Connect只能连接两条流
    val coMapStream: DataStream[Product with Serializable] = connectedStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    coMapStream.print("connect stream")
    env.execute("Connected Operator")
  }
}
