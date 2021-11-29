package com.bjke.flink.dataSet

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val lines: DataSet[String] = env.fromElements(
      "Who's there?",

      "I think I hear them. Stand, ho! Who's there?")

    val count: AggregateDataSet[(String, Int)] = lines.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    count.print();
  }

}
