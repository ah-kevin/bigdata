package com.bjke.flink.window

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 演示基于时间的滚动和滑动窗口
 */
object WidowDemo_1_2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    val lines: DataStream[String] = env.socketTextStream("localhost", 9527)
    val keyDs: KeyedStream[CartInfo, String] = lines.map { v =>
      val arr: Array[String] = v.split(",")
      CartInfo(arr(0), arr(1).toInt)
    }.keyBy(_.sensorId)

    //    val result1: DataStream[CartInfo] = keyDs
    //      // * 需求1:每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滚动窗口
    //      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    //      .sum(1)
    //    // * 需求2:每5秒钟统计一次，最近10秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滑动窗口
    //    val result2: DataStream[CartInfo] = keyDs
    //      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    //      .sum(1)

    // * 需求1:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现5次进行统计--基于数量的滚动窗口
    val result3: DataStream[CartInfo] = keyDs.countWindow(5).sum("count")
    // * 需求2:统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现3次进行统计--基于数量的滑动窗口
    val result4: DataStream[CartInfo] = keyDs.countWindow(5, 3).sum("count")

    /**
     * 1,5
     * 2,5
     * 3,5
     * 4,5
     */
    //    result1.print();
    //    result2.print();
    //    result2.print();
        result4.print();
    env.execute();
  }

  case class CartInfo(sensorId: String, count: Int) {}
}
