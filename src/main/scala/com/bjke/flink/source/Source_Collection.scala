package com.bjke.flink.source

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

object Source_Collection {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    import java.util
    //TODO 1.source
    val ds1: DataStream[String] = env.fromElements("hadoop spark flink", "hadoop spark flink")
    val ds2: DataStream[String] = env.fromCollection(Array("hadoop spark flink", "hadoop spark flink"))
    val ds3: DataStream[Long] = env.fromSequence(1, 100)
    ds1.print();
    ds2.print();
    ds3.print();

    env.execute();
  }

}
