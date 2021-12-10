package com.bjke.flink.watermaker

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.DateFormat
import java.time.Duration
import java.util
import java.util.UUID
import scala.util.Random

object WatermakerDemo01_Check {
  def main(args: Array[String]): Unit = {
    val df: FastDateFormat = FastDateFormat.getInstance("HH:mm:ss")
    // 1.env
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
          println(s"发送的数据为： ${userId}: ${df.format(eventTime)}")
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
    //    val orderDsWithWM: DataStream[Order] = orderDs.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Order](Duration.ofSeconds(3))
    //      .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
    //        // 指定事件时间列
    //        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = element.eventTime
    //      }))
    var orderDsWithWM: DataStream[Order] = orderDs.assignTimestampsAndWatermarks(new WatermarkStrategy[Order] {
      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Order] = super.createTimestampAssigner(context)

      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Order] = {
        new WatermarkGenerator[Order] {
          var userId = 0
          var eventTime = 0L;
          var outOfOrdernessMillis = 3000
          var maxTimestamp = Long.MinValue + outOfOrdernessMillis + 1;

          override def onEvent(event: Order, eventTimestamp: Long, output: WatermarkOutput): Unit = {
            userId = event.userId
            eventTime = eventTimestamp
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp)
          }

          override def onPeriodicEmit(output: WatermarkOutput): Unit = {
            val watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1)
            println("key:" + userId + ",系统时间:" + df.format(System.currentTimeMillis) + ",事件时间:" + df.format(eventTime) + ",水印时间:" + df.format(watermark.getTimestamp))
            output.emitWatermark(watermark)
          }
        }
      }
    }.withTimestampAssigner(new SerializableTimestampAssigner[Order] {
      // 指定事件时间列
      override def extractTimestamp(element: Order, recordTimestamp: Long): Long = element.eventTime
    }))

    val result: DataStream[String] = orderDsWithWM.keyBy(_.userId)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //      .sum("money")
      .apply(new WindowFunction[Order, String, Int, TimeWindow] {
        override def apply(key: Int, window: TimeWindow, orders: Iterable[Order], out: Collector[String]): Unit = {
          var list = new util.ArrayList[String]()
          for (order <- orders) {
            val eventTime: Long = order.eventTime
            val formatTime: String = df.format(eventTime)
            list.add(formatTime)
          }
          val start: String = df.format(window.getStart)
          val end: String = df.format(window.getEnd)
          //现在就已经获取到了当前窗口的开始和结束时间,以及属于该窗口的所有数据的事件时间,把这些拼接并返回
          val outStr: String = String.format("key:%s,窗口开始结束:[%s~%s),属于该窗口的事件时间:%s", key.toString, start, end, list.toString)
          out.collect(outStr)
        }
      })
    result.print()
    env.execute()
  }

  case class Order(orderId: String, userId: Int, money: Int, eventTime: Long) {}
}
