package com.bjke.flink.transformation

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

object TransformationApp {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    val lines: DataStream[String] = env.socketTextStream("localhost", 9527)
    flatMap(lines)
    env.execute();
  }

  def flatMap(lines: DataStream[String]) = {
    val result: DataStream[(String, Int)] = lines.flatMap {
      _.split(" ")
    }.filter(!_.equals("TMD"))
      .map((_, 1))
      .keyBy(_._1)
      //      .reduce((a, b) => (a._1, a._2 + b._2))
      .sum(1)
    result.print();
  }
}
