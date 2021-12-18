package com.bjke.flink.feature

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object BroadCastStateDemo {
  def main(args: Array[String]): Unit = {

  }

  class MySqlSource extends RichSourceFunction[Map[String, (String, Int)]] {
    var flag = true
    var conn: Connection = _
    var ps: PreparedStatement = _
    var rs: ResultSet = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink", "root", "890728")
      ps = conn.prepareStatement("""select userId,userName,userAge from user_info""")
    }

    override def run(ctx: SourceFunction.SourceContext[Map[String, (String, Int)]]): Unit = {

    }

    override def cancel(): Unit = {

    }
  }
}
