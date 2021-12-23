package com.bjke.flink.feature

import com.bjke.flink.model.{FactOrderItem, Goods, OrderItem}
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{RecordTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Random

object Join_WindowJoin {
  def main(args: Array[String]): Unit = {
    //    1.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    env.setParallelism(1)
    // 2. source
    // 商品数据流
    val goods$: DataStream[Goods] = env.addSource(GoodsSource)
    // 订单数据流
    val orderItem$: DataStream[OrderItem] = env.addSource(OrderItemSource)
    // 给数据添加水印（这里简单一点直接使用系统时间作为事件事件）
    //    val orderDsWithWM: DataStream[Order] = orderDs.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Order](Duration.ofSeconds(3))
    //      .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
    //        // 指定事件时间列
    //        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = element.eventTime
    //      }))
    val goodDSWithWaterMark$: DataStream[Goods] = goods$.assignTimestampsAndWatermarks(GoodsWatermark)
    val orderItemDSWithWaterMark$: DataStream[OrderItem] = orderItem$.assignTimestampsAndWatermarks(OrderItemWatermark)

    //TODO 2.transformation---这里是重点
    //商品类(商品id,商品名称,商品价格)
    //订单明细类(订单id,商品id,商品数量)
    //关联结果(商品id,商品名称,商品数量,商品价格*商品数量)
    val result: DataStream[FactOrderItem] = goodDSWithWaterMark$.join(orderItemDSWithWaterMark$)
      .where(_.goodsId)
      .equalTo(_.goodsId)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new JoinFunction[Goods, OrderItem, FactOrderItem] {
        override def join(first: Goods, second: OrderItem): FactOrderItem = {
          FactOrderItem(goodsId = first.goodsId, goodsName = first.goodsName, count = second.count,
            totalMoney = second.count * first.goodsPrice)
        }
      })
    result.print()
    env.execute();
  }

  object GoodsSource extends RichSourceFunction[Goods] {
    private var isCancel: Boolean = _

    override def open(parameters: Configuration): Unit = {
      isCancel = false
    }

    override def run(ctx: SourceFunction.SourceContext[Goods]): Unit = {
      while (!isCancel) {
        Goods.GOODS_LIST.toStream.foreach(ctx.collect)
        TimeUnit.SECONDS.sleep(1)
      }
    }

    override def cancel(): Unit = {
      isCancel = true
    }
  }

  object OrderItemSource extends RichSourceFunction[OrderItem] {
    private var isCancel: Boolean = _
    private var r: Random = _

    override def open(parameters: Configuration): Unit = {
      isCancel = false
      r = new Random()
    }

    override def run(ctx: SourceFunction.SourceContext[OrderItem]): Unit = {
      while (!isCancel) {
        val goods: Goods = Goods.randomGoods()
        val orderItem: OrderItem = OrderItem(UUID.randomUUID().toString, goods.goodsId, r.nextInt(10) + 1)
        ctx.collect(orderItem)
        orderItem.goodsId = "111"
        ctx.collect(orderItem)
        TimeUnit.SECONDS.sleep(1)
      }
    }

    override def cancel(): Unit = {
      isCancel = true
    }
  }

  //构建水印分配器，学习测试直接使用系统时间了
  object GoodsWatermark extends WatermarkStrategy[Goods] {
    override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Goods] = {
      (element: Goods, recordTimestamp: Long) => System.currentTimeMillis()
    }

    override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Goods] = {
      new WatermarkGenerator[Goods] {
        override def onEvent(event: Goods, eventTimestamp: Long, output: WatermarkOutput): Unit = {
          output.emitWatermark(new Watermark(System.currentTimeMillis()))
        }

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {
          output.emitWatermark(new Watermark(System.currentTimeMillis()))
        }
      }
    }
  }

  //构建水印分配器，学习测试直接使用系统时间了
  object OrderItemWatermark extends WatermarkStrategy[OrderItem] {
    override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[OrderItem] = {
      (element: OrderItem, recordTimestamp: Long) => System.currentTimeMillis()
    }

    override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[OrderItem] = {
      new WatermarkGenerator[OrderItem] {
        override def onEvent(event: OrderItem, eventTimestamp: Long, output: WatermarkOutput): Unit = {
          output.emitWatermark(new Watermark(System.currentTimeMillis()))
        }

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {
          output.emitWatermark(new Watermark(System.currentTimeMillis()))
        }
      }
    }
  }

}
