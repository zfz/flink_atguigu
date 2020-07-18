package com.atguigu.ch05_window

import com.atguigu.SensorEntity
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeAssigner {
  def main(args: Array[String]): Unit = {
    // val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val conf: Configuration = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 并行度1，方便观察
    env.setParallelism(1)
    // 指定eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置watermark生成的时间间隔
    env.getConfig.setAutoWatermarkInterval(300L)

    // 从Socket中读取数据
    val dataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorStream: DataStream[SensorEntity] = dataStream.map(
      data => {
        val fields = data.split(",")
        SensorEntity(fields(0), fields(1).trim.toLong, fields(2).trim.toDouble)
      })
      // 自定义Assigner
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorEntity](Time.seconds(1)) {
          override def extractTimestamp(t: SensorEntity): Long = t.timestamp * 1000
        })

    // 统计15s内的最小温度，滑动间隔5s
    val minTemperatureStream: DataStream[(String, Double)] = sensorStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      // 滑动窗口，5s是滑动步长
      .timeWindow(Time.seconds(15), Time.seconds(5))
      // reduce进行增量聚合
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    minTemperatureStream.print("min temperature stream")
    dataStream.print("input stream")

    env.execute("Event Time Assigner Watermark")
  }
}
