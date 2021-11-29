package com.bjke.flink.dataSet


import org.apache.flink.streaming.api.scala._

object WordCount2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lines: DataStream[String] = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")

    val count: DataStream[(String, Int)] = lines.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }.map((_, 1))
      .keyBy(_._1)
      .sum(1)
    count.print()
    env.execute("wordCount")

  }

}
