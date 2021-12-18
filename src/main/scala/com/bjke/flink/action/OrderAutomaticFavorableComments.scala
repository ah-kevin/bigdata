package com.bjke.flink.action

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util
import java.util.{Map, UUID}
import scala.util.Random

object OrderAutomaticFavorableComments {
  def main(args: Array[String]): Unit = {
    //    1.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    env.setParallelism(1)
    //    2.source
    val orderDs: DataStream[(String, String, Long)] = env.addSource(new MySource)
    //    3.transformation
    //    设置经过interval毫秒用户未对订单做出评价，自动给与好评.为了演示方便，设置5s的时间
    val interval: Long = 5000L; // 5s
    //    分组后使用自定义KeyedProcessFunction完成定时判断超时订单并自动好评
    orderDs.keyBy(_._1)
      .process(new TimeProcessFunction(interval));
    //      4.sink
    //    5.execute
    env.execute()
  }

  /**
   * 自定义source实时产生订单数据Tuple3<用户id,订单id, 订单生成时间>
   */
  class MySource extends SourceFunction[(String, String, Long)] {
    var flag = true
    val random = new Random()

    override def run(sourceContext: SourceFunction.SourceContext[(String, String, Long)]): Unit = {
      while (flag) {
        val userId: String = random.nextInt(5) + ""
        val orderId: String = UUID.randomUUID().toString
        val currentTimeMillis: Long = System.currentTimeMillis()
        sourceContext.collect((userId, orderId, currentTimeMillis))
        Thread.sleep(500)
      }
    }

    override def cancel(): Unit = {
      flag = false
    }
  }

  /**
   * 自定义ProcessFunction完成订单自动好评
   * 进来一条数据应该在interval时间后进行判断该订单是否超时是否需要自动好评
   * abstract class KeyedProcessFunction<K, I, O>
   */
  class TimeProcessFunction(interval: Long) extends KeyedProcessFunction[String, (String, String, Long), Long] {
    var mapState: MapState[String, Long] = _

    //1. 初始化
    override def open(parameters: Configuration): Unit = {
      val mapStateDesc: MapStateDescriptor[String, Long] = new MapStateDescriptor[String, Long]("mapState", classOf[String], classOf[Long])
      mapState = getRuntimeContext.getMapState(mapStateDesc)
    }


    //2。 处理每一条数据
    override def processElement(value: (String, String, Long), ctx: KeyedProcessFunction[String, (String, String, Long), Long]#Context, out: Collector[Long]): Unit = {
      //<用户id,订单id, 订单生成时间> value里面是当前进来的数据里面有订单生成时间
      // 把订单保存到状态中
      mapState.put(value._2, value._3)
      // 该订单在value._2 + interval/到期，这时候如果没有评价的话需要系统自动给予默认好评
      // 注册一个定时器在value._2 + interval 时检查是否需要默认好评
      ctx.timerService().registerProcessingTimeTimer(value._3 + interval)
    }

    //3.执行定时任务
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String, Long), Long]#OnTimerContext, out: Collector[Long]): Unit = {
      // 检查历史订单数据
      // 取出状态中的
      val iterator: util.Iterator[util.Map.Entry[String, Long]] = mapState.iterator()
      while (iterator.hasNext) {
        val map: util.Map.Entry[String, Long] = iterator.next()
        val orderId: String = map.getKey
        val orderTime: Long = map.getValue
        // 先判断是否好评 -- 实际中应该去调用订单评价系统看是否好评了,我们这里写个方法模拟下
        if (!isFavorable(orderId)) { // 该订单没有好评
          // 判断是否超时
          if (System.currentTimeMillis() - orderTime >= interval) {
            println(s"order:${orderId} 该订单已经超时未评价，系统自动给予好评。。。")
          }
        } else {
          println("该订单已经评价。。。")
        }
        // 遗传状态中的数据，避免后续重复判断
        iterator.remove();
        mapState.remove(orderId)
      }
    }

    // 自定义一个方法模拟订单系统是否返回该订单是否已经好评
    def isFavorable(orderId: String): Boolean = {
      orderId.hashCode() % 2 == 0
    }
  }
}
