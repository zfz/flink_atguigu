package com.atguigu.wc

// 隐式转换
import org.apache.flink.api.scala._

object WordCount {
  // 批处理word count程序
  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "src/main/resources/hello.txt"
    val input = env.readTextFile(inputPath)

    // 切分数据得到word，然后聚合
    val wordCntDataSet = input.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    wordCntDataSet.print()
  }
}
