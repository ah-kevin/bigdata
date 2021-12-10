package com.bjke.flink.dataSet

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala._

object WordCount3 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    val lines: DataStream[String] = env.socketTextStream("localhost", 9527)

    val count: DataStream[(String, Int)] = lines.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }.map {
      (_, 1)
    }
      .keyBy(_._1)
      .sum(1)
    count.print()
    env.execute("wordCount")
  }

}