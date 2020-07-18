package com.atguigu.ch04_sink

import com.atguigu.SensorEntity
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object ToKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 从自定义的集合中读取数据
    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444)
    ))

    val dataStream: DataStream[String] = sensorStream.map {
      data => s"${data.id}, ${data.timestamp}, ${data.temperature}"
    }
    dataStream.print("sensor string")

    // 向kafka发送消息
    dataStream.addSink(new FlinkKafkaProducer011[String](
      "localhost:9092",
      "sensor",
      new SimpleStringSchema())
    )

    env.execute("ToKafka")

  }
}
