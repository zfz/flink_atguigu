package com.atguigu.source

import com.atguigu.SensorEntity
import org.apache.flink.streaming.api.scala._

object FromFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputPath = "src/main/resources/sensor.txt"

    // 从自定义的集合中读取数据
    val sensorStream: DataStream[SensorEntity] = env.readTextFile(inputPath).map(item => {
      val fields = item.split(",")
      SensorEntity(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
    })

    sensorStream.print("sensor reading from text").setParallelism(2)

    env.execute("FromFile")
  }
}
