package com.bjke.flink.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions}

object Demo2 {


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    val wordsDS: DataStream[WC] = env.fromElements(
      WC("hello", 1),
      WC("word", 1),
      WC("hello", 1))
    //    sqlView(wordsDS, tenv)
    table(tenv, wordsDS)
    env.execute()
  }

  def table(tenv: StreamTableEnvironment, wordsDS: DataStream[WC]) = {
    val table1: Table = wordsDS.toTable(tenv, $"word", $"frequency")
    val resultTable: Table = table1.groupBy($"word")
      .select($("word"), $("frequency").sum().as("frequency"))
//      .filter($"frequency" >= 2)
    val resultDs$: DataStream[(Boolean, WC)] = tenv.toRetractStream[WC](resultTable)
    resultDs$.print()
  }

  def sqlView(wordsDS: DataStream[WC], tenv: StreamTableEnvironment): Unit = {
    // 将datastream转换为table和view
    tenv.createTemporaryView("t_words", wordsDS, $"word", $"frequency")
    val table: Table = tenv.sqlQuery("""select word,sum(frequency) as frequency from t_words group by word""")
    // 转为ds
    //toAppendStream → 将计算后的数据append到结果DataStream中去
    //toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上更新true或是删除false
    val resultDs$: DataStream[(Boolean, WC)] = tenv.toRetractStream[WC](table)
    resultDs$.print()
  }


  case class WC(word: String, frequency: Int)
}
