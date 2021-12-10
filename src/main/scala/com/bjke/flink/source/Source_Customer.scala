package com.bjke.flink.source

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.types.Nothing

import java.util.UUID
import scala.util.Random

// 自定义随机生成数据
object Source_Customer {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    val orderDs: DataStream[Order] = env.addSource(new MyOrderSource()).setParallelism(2)

    orderDs.print();

    env.execute();
  }


  class MyOrderSource extends RichParallelSourceFunction[Order] {
    var flag = true

    // 执行并生产数据
    override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
      val random = new Random()
      while (flag) {
        val oid: String = UUID.randomUUID().toString
        val userId: Int = random.nextInt(3)
        val money: Int = random.nextInt(101)
        val createTime: Long = System.currentTimeMillis()
        ctx.collect(Order(oid, userId, money, createTime))
        Thread.sleep(1000)
      }
    }

    // 执行cancel命令时候执行
    override def cancel(): Unit = {
      flag = false
    }
  }

  case class Order(id: String, userId: Int, money: Int, createTime: Long) {


  }

}
