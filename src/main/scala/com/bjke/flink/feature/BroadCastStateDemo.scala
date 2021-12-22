package com.bjke.flink.feature

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.{sql, util}
import java.sql.{Connection, DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Random

object BroadCastStateDemo {

  def main(args: Array[String]): Unit = {
    //    1.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    env.setParallelism(1)
    //    2.source
    //    -1.构建实时数据事件流- 数据量较大-自定义随机
    val eventDS$: DataStream[(String, String, String, Int)] = env.addSource(new MySource)
    //        <userID, eventTime, eventType, productID>

    //    -2.构建配置流-从MySQL
    //    <用户id,<姓名,年龄>>
    val userDS$: DataStream[util.Map[String, (String, Int)]] = env.addSource(new MySqlSourceRich)
    //     3.transformation
    //    -1.定义状态描述器
    val descriptor: MapStateDescriptor[Void, util.Map[String, (String, Int)]] = new MapStateDescriptor("info", classOf[Void], classOf[util.Map[String, (String, Int)]])
    //    -2.广播配置流
    val broadcastDS$: BroadcastStream[util.Map[String, (String, Int)]] = userDS$.broadcast(descriptor)
    //    -3.将事件流和广播流进行连接
    val connectDS$: BroadcastConnectedStream[(String, String, String, Int), util.Map[String, (String, Int)]] = eventDS$.connect(broadcastDS$)
    //    -4.处理连接后的流-根据配置流补全事件流中的用户的信息
    val result: DataStream[(String, String, String, Int, String, Int)] = connectDS$.process(new BroadcastProcessFunction[
      //    <userID, eventTime, eventType, productID> 事件流
      (String, String, String, Int),
      // <用户id,<姓名,年龄>> 广播流
      util.Map[String, (String, Int)],
      // 结果流
      (String, String, String, Int, String, Int)] {
      // 处理事件流中的每一个元素

      override def processElement(value: (String, String, String, Int), ctx: BroadcastProcessFunction[(String, String, String, Int), util.Map[String, (String, Int)], (String, String, String, Int, String, Int)]#ReadOnlyContext, out: Collector[(String, String, String, Int, String, Int)]): Unit = {
        //value 就是事件流中的数据
        // 目标就是将value和广播流中的数据进行关联，返回结果流
        // <userID, eventTime, eventType, productID> 事件流-已经有了
        // <用户id,<姓名,年龄>> 广播流-需要获取

        // 获取广播流
        val broadState: ReadOnlyBroadcastState[Void, util.Map[String, (String, Int)]] = ctx.getBroadcastState(descriptor)
        // <用户id,<姓名,年龄>>
        val map: util.Map[String, (String, Int)] = broadState.get(null) // 广播流中的数据
        if (map != null) {
          // 根据value中的用户id去map获取用户信息
          val userId: String = value._1
          val tuple: (String, Int) = map.get(userId)
          val username: String = tuple._1
          val age: Int = tuple._2

          // 收集数据
          out.collect((userId, value._2, value._3, value._4, username, age))
        }
      }


      // 更新广播流中的数据
      override def processBroadcastElement(value: util.Map[String, (String, Int)], ctx: BroadcastProcessFunction[(String, String, String, Int), util.Map[String, (String, Int)], (String, String, String, Int, String, Int)]#Context, out: Collector[(String, String, String, Int, String, Int)]): Unit = {
        // value就是从mysql中每隔5查询出来并广播到状态中最新数据
        // 要把最新到数据放到state中
        val broadState: BroadcastState[Void, util.Map[String, (String, Int)]] = ctx.getBroadcastState(descriptor);
        broadState.clear();
        broadState.put(null, value)
      }
    })
    //. sink
    result.print()
    //    5.execute
    env.execute()
  }

  /**
   * 随机事件流--数据量较大
   * 用户id,时间,类型,产品id
   * <userID, eventTime, eventType, productID>
   */
  class MySource extends SourceFunction[(String, String, String, Int)] {
    var isRunning = true

    override def run(ctx: SourceFunction.SourceContext[(String, String, String, Int)]): Unit = {
      val random = new Random()
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      while (isRunning) {
        val id: Int = random.nextInt(4) + 1
        val user_id: String = "user_" + id
        val eventTime: String = df.format(new Date())
        val eventType: String = "type_" + random.nextInt(3)
        val productId: Int = random.nextInt(4)
        ctx.collect((user_id, eventTime, eventType, productId))
        Thread.sleep(500)
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }

  /**
   * 配置流/规则流/用户信息流--数量较小
   * <用户id,<姓名,年龄>>
   */
  class MySqlSourceRich extends RichSourceFunction[util.Map[String, (String, Int)]] {
    var flag = true
    var conn: Connection = _
    var ps: sql.PreparedStatement = _
    var rs: ResultSet = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink?useSSL=false", "root", "890728")
      ps = conn.prepareStatement("""select userId,userName,userAge from user_info""")
    }

    override def run(ctx: SourceFunction.SourceContext[util.Map[String, (String, Int)]]): Unit = {
      while (flag) {
        var map: util.HashMap[String, (String, Int)] = new util.HashMap[String, (String, Int)]()
        rs = ps.executeQuery()
        while (rs.next()) {
          val userId: String = rs.getString("userId")
          val userName: String = rs.getString("userName")
          val userAge: Int = rs.getInt("userAge")
          map.put(userId, (userName, userAge))
        }
        ctx.collect(map)
        Thread.sleep(5000) // 每隔5s更新一下用户的配置信息
      }
    }

    override def cancel(): Unit = {
      flag = false
    }

    override def close(): Unit = {
      if (conn != null) {
        conn.close()
      }
      if (ps != null) {
        ps.close()
      }
      if (rs != null) {
        rs.close()
      }
    }
  }

}
