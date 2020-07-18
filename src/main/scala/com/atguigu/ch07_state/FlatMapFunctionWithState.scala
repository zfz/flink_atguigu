package com.atguigu.ch07_state

import com.atguigu.SensorEntity
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object FlatMapFunctionWithState {
  def main(args: Array[String]): Unit = {
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

    // 方式3.使用flatMapWithState
    // 第一个泛型是返回值，第二个参数是状态类型
    val processedStream = sensorStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double]{
        // 初始情况，第一次接收
        case (input: SensorEntity, None) => (List.empty, Some(input.temperature))
        // 第二次接收，比较上次状态
        case (input: SensorEntity, lastTemp: Some[Double]) => {
          val diff = (input.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
          } else {
            (List.empty, Some(input.temperature))
          }
        }

      }

    dataStream.print("input data")
    processedStream.print("processed stream")

    env.execute("Flatmap with State")
  }
}
