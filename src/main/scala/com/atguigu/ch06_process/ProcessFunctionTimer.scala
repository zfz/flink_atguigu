package com.atguigu.ch06_process

import com.atguigu.SensorEntity
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTimer {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env enableCheckpointing(6000)

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

    val processedStream: DataStream[String] = sensorStream.keyBy(_.id)
      .process(new TempIncreaseAlertFunction())

    dataStream.print("input data")
    processedStream.print("processed stream")

    env.execute("Process Function Timer")
  }
}

/**
 * 自定义处理函数，用于监控传感器的温度，如果在10s内连续上升，则报警
 */
class TempIncreaseAlertFunction() extends KeyedProcessFunction[String, SensorEntity, String] {
  // 定义一个状态，用来保存上一个状态的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val curTimer: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("curTimer",  classOf[Long]))

  override def processElement(cur: SensorEntity,
                              ctx: KeyedProcessFunction[String, SensorEntity, String]#Context,
                              out: Collector[String]): Unit = {
    // 先取出上一个温度值
    val preTemp = lastTemp.value()

    // 更新当前温度
    lastTemp.update(cur.temperature)

    if(preTemp == 0.0 || cur.temperature <= preTemp) {
      // 如果温度下降，则取消定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimer.value())
      curTimer.clear()
    } else if(cur.temperature > preTemp && curTimer.value() == 0L) {
      // 如果温度上升，注册定时器
      // 开启一个3s的定时器，这里依据需要，可创建EventTimeTimer
      val timestamp: Long = ctx.timerService().currentProcessingTime() + 3000
      ctx.timerService().registerProcessingTimeTimer(timestamp)
      curTimer.update(timestamp)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorEntity, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 输出报警信息
    out.collect(ctx.getCurrentKey() + "温度连续上升")
    curTimer.clear()
  }
}