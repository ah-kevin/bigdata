package com.bjke.flink.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions}
import org.apache.flink.table.api._

object Demo1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val orderA: DataStream[Order] = env.fromCollection(Array(Order(1L, "beer", 3), Order(1L, "diaper", 4), Order(3L, "rubber", 2)))
    val orderB: DataStream[Order] = env.fromCollection(Array(Order(2L, "pen", 3), Order(2L, "rubber", 3), Order(4L, "beer", 1)))

    // 将datastream转换为table和view
    val tableA: Table = orderA.toTable(tenv, $"user", $"product", $"amount")
    tableA.printSchema();
    print(tableA)
    val table2: Table = orderB.toTable(tenv, $"user", $"product", $"amount")
    tenv.createTemporaryView("tableB", table2)
    // 查询tableA中amount>2 和tableB中>1的数据最后合并
    /**
     * select * from tableA where amount>2 union select * from tableB where amount>1
     */
    val resultTable: Table = tenv.sqlQuery(s"""select * from ${tableA} where amount>2 union select * from tableB where amount>1""")
    resultTable.printSchema()
    println(resultTable)
    // table转为datastream
    //    val resultDs: DataStream[Order] = tenv.toAppendStream[Order](resultTable)
    val resultDs: DataStream[(Boolean, Order)] = tenv.toRetractStream[Order](resultTable)
    //toAppendStream → 将计算后的数据append到结果DataStream中去
    //toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上更新true或是删除false
    //类似StructuredStreaming中的append/update/complete
    resultDs.print()

    env.execute()

  }


  case class Order(user: Long, product: String, amount: Int)
}
