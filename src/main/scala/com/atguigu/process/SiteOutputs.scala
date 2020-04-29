package com.atguigu.process

import com.atguigu.SensorEntity
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SiteOutputs {
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

    val processedStream: DataStream[SensorEntity] = sensorStream
      .process(new FreezingMonitor())

    // 打印主流
    processedStream.print("processed stream")
    // 打印测输出流
    processedStream.getSideOutput(new OutputTag[SensorEntity]("freezing-alerts")).print("freezing stream")

    env.execute("Side Outputs")
  }
}

/**
 *  华氏温度低于32度（0度），作为低温，第一个泛型是输入类型，第二个泛型是输出类型
 */
class FreezingMonitor() extends ProcessFunction[SensorEntity, SensorEntity] {
  // 定义一个侧输出流标签，需要指定侧输出流的类型
  lazy val freezingOutput: OutputTag[SensorEntity] = new OutputTag[SensorEntity]("freezing-alerts")

  override def processElement(cur: SensorEntity,
                              ctx: ProcessFunction[SensorEntity, SensorEntity]#Context,
                              out: Collector[SensorEntity]): Unit = {
    if (cur.temperature < 32.0) {
      ctx.output(freezingOutput, cur)
    } else {
      out.collect(cur)
    }
  }
}