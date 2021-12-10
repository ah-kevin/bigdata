package com.bjke.flink.table

import com.bjke.flink.model.Order
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions}
import org.apache.flink.types.Row

import java.time.Duration
import java.util.UUID
import scala.util.Random

/**
 * 演示Flink Table&SQL 案例- 使用事件时间+Watermaker+window完成订单统计
 */
object Demo3 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val orderDs$: DataStream[Order] = env.addSource(new SourceFunction[Order] {
      var flag = true

      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        val random = new Random()
        while (flag) {
          val oid: String = UUID.randomUUID().toString
          val userId: Int = random.nextInt(3)
          val money: Int = random.nextInt(101)
          // 随机模拟延迟
          val eventTime: Long = System.currentTimeMillis()
          Thread.sleep(1000)
          ctx.collect(Order(oid, userId, money, eventTime))
        }
      }

      override def cancel(): Unit = {
        flag = false
      }
    })
    //需求:事件时间+Watermarker+FlinkSQL和Table的window完成订单统计
    val orderDsWithWaterMark: DataStream[Order] = orderDs$.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Order](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
        // 指定事件时间列
        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = element.eventTime
      }))
    // datastream->view/table 指定列的时候需要指定哪一列是时间
    tenv.createTemporaryView("t_order", orderDsWithWaterMark, $"orderId", $"userId", $"money", $"eventTime".rowtime())
    /*
    select  userId, count(orderId) as orderCount, max(money) as maxMoney,min(money) as minMoney
    from t_order
    group by userId,
    tumble(eventTime, INTERVAL '5' SECOND)
     */
    val resultTable: Table = tenv.sqlQuery(
      """select userId, count(orderId) as orderCount, max(money) as maxMoney,min(money) as minMoney
        |    from t_order
        |    group by userId,
        |    tumble(eventTime, INTERVAL '5' SECOND)""".stripMargin)
    resultTable.printSchema()
    val resultDs: DataStream[(Boolean, Row)] = tenv.toRetractStream[Row](resultTable)
    resultDs.print();
    env.execute();
  }
}
