package com.bjke.flink.action

import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Random

/**
 * Desc
 * 1.实时计算出当天零点截止到当前时间的销售总额 11月11日 00:00:00 ~ 23:59:59
 * 2.计算出各个分类的销售top3
 * 3.每秒钟更新一次统计结果
 */
object DoubleElevenBigScreen {

  def main(args: Array[String]): Unit = {
    //    1.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    //    2.source
    val orderDS$: DataStream[(String, Double)] = env.addSource(new MySource)
    //    3.transformation--预聚合 每隔1s聚合一下各个分类的销售总金额
    //    3.1定义大小为一天的窗口,第二个参数表示中国使用的UTC+08:00时区比UTC时间早
    //      keyBy(t->t.f0)
    val tempAggResult: DataStream[CategoryPojo] = orderDS$.keyBy(_._1)
      //     表示每隔一天计算一次
      //      .window(TumblingProcessingTimeWindows.of(Time.days(1)))
      // 表示每隔1s，计算最近一天的数据，但是11月11 00.01:00 计算的是11月10日00.01:00~11月11日00:01:00->不对
      //      .window(SlidingProcessingTimeWindows.of(Time.days(1), Time.seconds(1)));
      //*例如中国使用UTC+08:00，您需要一天大小的时间窗口，
      //*窗口从当地时间的00:00:00开始，您可以使用{@code of(时间.天(1),时间.hours(-8))}.
      //下面的代码表示从当天的00:00:00开始计算当天的数据,缺一个触发时机/触发间隔
      //3.1定义大小为一天的窗口,第二个参数表示中国使用的UTC+08:00时区比UTC时间早
      .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
      //      .trigger()
      //    3.2定义一个1s的触发器
      .trigger(ContinuousProcessingTimeTrigger.of[TimeWindow](Time.seconds(1)))
      //          .sum() // 简单聚合
      //    3.3聚合结果.aggregate(new PriceAggregate(), new WindowResult());
      .aggregate(new PriceAggregate(), new WindowResult())
    //    3.4看一下聚合的结果
    tempAggResult.print("初步聚合的各个分类的销售额");
    //      CategoryPojo(category=男装, totalPrice=17225.26, dateTime=2020-10-20 08:04:12)

    //    4.sink-使用上面预聚合的结果,实现业务需求:
    tempAggResult.keyBy(_.dateTime)
      //每秒钟更新一次统计结果
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1))) // 每隔1s进行嘴周的聚合并输出结果
      //在ProcessWindowFunction中实现该复杂业务逻辑
      .process(new FinalResultWindowProcess());
    //    5.execute
    env.execute();
  }

  class MySource extends SourceFunction[(String, Double)] {
    var flag = true
    val categorys: Array[String] = Array("女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公")
    val random = new Random()

    override def run(sourceContext: SourceFunction.SourceContext[(String, Double)]): Unit = {
      while (flag) {
        val i: Int = random.nextInt(categorys.length)
        val category: String = categorys(i) // 获取随机分类
        val price: Double = random.nextDouble() * 100 //注意nextDouble生成的是[0~1)之间的随机数,*100之后表示[0~100)
        sourceContext.collect((category, price))
        Thread.sleep(20)
      }
    }

    override def cancel(): Unit = {
      flag = false
    }
  }

  // [IN, OUT, KEY, W <: Window]
  class FinalResultWindowProcess extends ProcessWindowFunction[CategoryPojo, Object, String, TimeWindow] {
    // key: 表示当前这1秒的时间
    // elements 表示戒指到当前这1s到各个分类到销售数据
    override def process(key: String, context: Context, elements: Iterable[CategoryPojo], out: Collector[Object]): Unit = {
      // 1.实时计算出当天零点截止到当前时间的销售总额 11月11日 00:00:00 ~ 23:59:59
      var total: Double = 0D // 用来记录销售总额
      // 2.计算出各个分类的销售top3: 如：  "女装"：10000, "男装"：9000, "图书"：8000
      // 注意： 这里只需要要求top3，也就是只需要排前3名就行，其他的不用管
      // 所以直接使用小顶堆完成top3排序：
      // 70
      // 80
      // 90
      // 如果进来一个比堆顶元素还有小的，直接不要
      // 如85，直接把堆定元素干掉,把85加进去并继续按照小顶堆规则排序，小的在上面，大的在下面
      // 80 85 90 创建一个小顶堆
      // 3.每秒钟更新一次统计结果
      val queue: mutable.PriorityQueue[CategoryPojo] = mutable.PriorityQueue[CategoryPojo]()(Ordering.by(_.totalPrice))
      for (element <- elements) {
        val price: Double = element.totalPrice
        total += price
        if (queue.size < 3) {
          queue += element // 或offer入队
        } else {
          if (price > queue.head.totalPrice) {
            //             queue.remove(queue.peek())
            queue.dequeue() // 移出堆顶元素'
            queue += element // 或offer入队
          }
        }
      }
      val top3List: List[String] = queue.toStream
        .sortBy(_.totalPrice)
        .reverse
        .map((c) => s"分类:${c.category}, 金额: ${c.totalPrice}")
        .toList
      println(s"时间:${key}  总金额: ${total} ")
      println("top3:" + StringUtils.join(top3List, "\n"))
    }
  }


  class PriceAggregate extends AggregateFunction[(String, Double), Double, Double] {
    // 初始化累加器
    override def createAccumulator(): Double = {
      return 0D
    }

    // 把数据累加到累加器上
    override def add(in: (String, Double), acc: Double): Double = {
      return in._2 + acc
    }

    // 获取累加结果
    override def getResult(acc: Double): Double = {
      return acc
    }

    //合并各个subtask结果
    override def merge(acc: Double, acc1: Double): Double = {
      return acc + acc1
    }
  }

  class WindowResult() extends WindowFunction[Double, CategoryPojo, String, TimeWindow] {
    val df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    override def apply(category: String, window: TimeWindow, input: Iterable[Double], out: Collector[CategoryPojo]): Unit = {
      val currentTimeMillis: Long = System.currentTimeMillis()
      val dateTime: String = df.format(currentTimeMillis);
      val totalPrice: Double = input.iterator.next()
      out.collect(CategoryPojo(category, totalPrice, dateTime))
    }

  }

  //分类名称
  //该分类总销售额
  // 截止到当前时间的时间,本来应该是EventTime,但是我们这里简化了直接用当前系统时间即可
  case class CategoryPojo(category: String, totalPrice: Double, dateTime: String)
}
