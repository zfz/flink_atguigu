package com.atguigu.ch05_window

import com.atguigu.SensorEntity
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeAscending {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 并行度1，方便观察
    env.setParallelism(1)

    // 指定eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从Socket中读取数据
    val dataStream: DataStream[String] = env.socketTextStream("localhost",7777)

    val sensorStream: DataStream[SensorEntity] = dataStream.map(
      data => {
        val fields = data.split(",")
        SensorEntity(fields(0), fields(1).trim.toLong, fields(2).trim.toDouble)
      })
      // 注意是ms，默认接收到的时间戳是升序，watermark的延时时间为0，这个方法只能处理升序数据，但是现实情况数据往往存在乱序
      .assignAscendingTimestamps(_.timestamp * 1000)

    // 统计10s内的最小温度
    val minTemperatureStream: DataStream[(String, Double)] = sensorStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      // 开时间窗口
      .timeWindow(Time.seconds(10))
      // reduce进行增量聚合
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    // 接收到新一条数据后，才会输出最小值
    minTemperatureStream.print("min temperature stream")
    dataStream.print("input stream")

    env.execute("Event Time Ascending TS")
  }
}
