package com.atguigu.ch05_window

import com.atguigu.SensorEntity
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWindowExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 从Socket中读取数据
    val sensorStream: DataStream[String] = env.socketTextStream("localhost",7777)

    // 统计10s内的最小温度
    val minTemperatureWindowStream: DataStream[(String, Double)] = sensorStream
      .map(data => {
        val fields: Array[String] = data.split(",")
        (fields(0), fields(2).trim.toDouble)})
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) // 开时间窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))  // reduce进行增量聚合

    minTemperatureWindowStream.print("min temperature")

    sensorStream.print("input data")

    env.execute("Time Window Example")
  }
}
