package com.bjke.flink.watermaker

import com.bjke.flink.source.Source_Customer.Order
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util.UUID
import scala.util.Random

object WatermakerDemo01 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    val orderDs: DataStream[Order] = env.addSource(new SourceFunction[Order] {
      var flag = true

      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        val random = new Random()
        while (flag) {
          val oid: String = UUID.randomUUID().toString
          val userId: Int = random.nextInt(2)
          val money: Int = random.nextInt(101)
          // 随机模拟延迟
          val eventTime: Long = System.currentTimeMillis() - random.nextInt(5) * 1000
          ctx.collect(Order(oid, userId, money, eventTime))
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = {
        flag = false
      }
    })
    // transformation 基于事件事件进行窗口计算。
    //注意:下面的代码使用的是Flink1.12中新的API
    //每隔5s计算最近5s的数据求每个用户的订单总金额,要求:基于事件时间进行窗口计算+Watermaker
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//在新版本中默认就是EventTime
    //设置Watermaker = 当前最大的事件时间 - 最大允许的延迟时间或乱序时间
    ///指定maxOutOfOrderness最大无序度/最大允许的延迟时间/乱序时间
    val orderDsWithWM: DataStream[Order] = orderDs.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Order](Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
        // 指定事件时间列
        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = element.eventTime
      }))

    val result: DataStream[Order] = orderDsWithWM.keyBy(_.userId)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum("money")
    result.print()
    env.execute()
  }

  case class Order(orderId: String, userId: Int, money: Int, eventTime: Long) {}
}
