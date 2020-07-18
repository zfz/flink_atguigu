package com.atguigu.ch02_source

import com.atguigu.SensorEntity
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object CustomSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 自定义Source
    val customStream: DataStream[SensorEntity] = env.addSource(new SensorSource)

    customStream.print().setParallelism(2)
    env.execute("CustomSource")
  }

  class SensorSource extends SourceFunction[SensorEntity] {
    // 定义一个flag，标志数据源是否正常运行
    var running: Boolean = true

    override def run(sourceContext: SourceFunction.SourceContext[SensorEntity]): Unit = {
      // 初始化一个随机数生成器
      val rand = new Random()

      // 初始化一组传感器温度数据
      var curTemp = 1.to(5).map {
        i => ("s" + i, 60 + rand.nextGaussian()*20)
      }

      // 产生数据流
      while (running) {
        // 在前一次温度的基础上更新数据流
        curTemp.map {
          t => (t._1, t._2 + rand.nextGaussian())
        }

        val curTimeStamp = System.currentTimeMillis()
        curTemp.foreach(
          t => sourceContext.collect(SensorEntity(t._1, curTimeStamp, t._2))
        )

        // 设置时间间隔
        Thread.sleep(5000)
      }
    }

    // 取消数据源生成数据
    override def cancel(): Unit = {
      running = false
    }
  }

}

