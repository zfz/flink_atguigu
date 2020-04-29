package com.atguigu.window

import com.atguigu.SensorEntity
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimePeriodicAssigner {
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
    env.getConfig.setAutoWatermarkInterval(5000L)

    // 从Socket中读取数据
    val dataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorStream: DataStream[SensorEntity] = dataStream.map(
      data => {
        val fields = data.split(",")
        SensorEntity(fields(0), fields(1).trim.toLong, fields(2).trim.toDouble)
      })
      // 自定义Assigner
      .assignTimestampsAndWatermarks(new PeriodicAssigner())

    // 统计10s内的最小温度
    val minTemperatureStream: DataStream[(String, Double)] = sensorStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      // 滚动窗口
      .timeWindow(Time.seconds(5))
      // reduce进行增量聚合
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    minTemperatureStream.print("min temperature stream")
    dataStream.print("input stream")

    env.execute("Event Time Custom Assigner")
  }

}


/**
 * 周期性生成一个waterMark
 */
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorEntity] {

  // 1 sec in ms
  val bound: Long = 1 * 1000
  // the maximum observed timestamp
  // Shouldn't use Long.MinValue, will cause overflow
  var maxTs: Long = -1

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(r: SensorEntity, previousTS: Long): Long = {
    // update maximum timestamp
    maxTs = maxTs.max(r.timestamp * 1000)
    // return record timestamp
    r.timestamp * 1000
  }
}