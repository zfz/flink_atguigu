package com.atguigu.ch07_state

import com.atguigu.SensorEntity
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StatefulProcessFunction {
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
      .process(new TempChangeAlert(10.0))

    dataStream.print("input data")
    processedStream.print("processed stream")

    env.execute("Stateful Process Function")
  }
}

/**
 * 方式1.使用KeyedProcessFunction
 * 功能：2次温度之间超过一定温度报警
 * 返回值：id，上次温度，本次温度
 * 包含上下文，用RichFunction记录状态
 */
class TempChangeAlert(threshold: Double)
  extends KeyedProcessFunction[String, SensorEntity, (String, Double, Double)] {
  // 定义一个状态变量，保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))


  override def processElement(cur: SensorEntity,
                              ctx: KeyedProcessFunction[String, SensorEntity, (String, Double, Double)]#Context,
                              out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次温度值
    val preTemp = lastTempState.value()
    // 求当前温度与上次温度差值
    val diff = (cur.temperature - preTemp).abs
    if (diff > threshold) {
      out.collect(cur.id, preTemp, cur.temperature)
    }
    // 更新上次温度
    lastTempState.update(cur.temperature)
  }
}