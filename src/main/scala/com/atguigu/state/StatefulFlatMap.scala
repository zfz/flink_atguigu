package com.atguigu.state

import com.atguigu.SensorEntity
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StatefulFlatMap {
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

    val processedStream = sensorStream.keyBy(_.id)
      .flatMap(new TempChangeAlert2(10.0))

    dataStream.print("input data")
    processedStream.print("processed stream")

    env.execute("Stateful Flatmap")
  }
}

/**
 * 方式2.使用RichFlatMapFunction
 * 功能：2次温度之间超过一定温度报警
 * 返回值：id，上次温度，本次温度
 * 由于没有key
 * 不使用FlatMapFunction的原因是没有包含上下文，无法记录状态
 *
 */
class TempChangeAlert2(threshold: Double) extends RichFlatMapFunction[SensorEntity, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明State变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(in: SensorEntity, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次温度值
    val preTemp = lastTempState.value()
    // 求当前温度与上次温度差值
    val diff = (in.temperature - preTemp).abs
    if (diff > threshold) {
      out.collect(in.id, preTemp, in.temperature)
    }
    // 更新上次温度
    lastTempState.update(in.temperature)
  }
}
