package com.atguigu.wc

// 隐式转换需要
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  // 流处理word count程序
  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收一个socket文本流
    // val dataStream = env.socketTextStream("localhost", 7777)
    // 从参数--host 和 --port 中获取
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val (host,port) = (tool.get("host") , tool.getInt("port"))
    val dataStream = env.socketTextStream(host, port)

    val wordCntStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)  // DataSet有groupBy操作，在DataStream中有keyBy操作相似的效果
      .sum(1)

    wordCntStream.print().setParallelism(2)

    // 启动executor
    env.execute("StreamWordCount")
  }
}