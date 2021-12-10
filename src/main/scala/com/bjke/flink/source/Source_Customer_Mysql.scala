package com.bjke.flink.source

import com.bjke.flink.model.RecordDetail
import com.google.gson.Gson
import org.apache.derby.iapi.sql.PreparedStatement
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

import java.sql
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.UUID
import scala.util.Random

// 自定义随机生成数据
object Source_Customer_Mysql {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    val detailDs: DataStream[RecordDetail] = env.addSource(new MySqlSource()).setParallelism(1);

    detailDs.print();

    env.execute();
  }


  class MySqlSource extends RichParallelSourceFunction[RecordDetail] {
    var ps: sql.PreparedStatement = _
    var conn: Connection = _
    var flag = true

    // open只执行一次，时刻开启资源
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://47.117.114.66:3306/werewolf_test?useSSL=false", "root", "890728")
      val sql = "select * from record_details order by id"
      ps = conn.prepareStatement(sql)
    }

    override def run(ctx: SourceFunction.SourceContext[RecordDetail]): Unit = {

//      while (flag) {
        val rs: ResultSet = ps.executeQuery()
        while (rs.next()) {
          ctx.collect(RecordDetail(
            rs.getInt("id"),
            rs.getString("created_at"),
            rs.getString("updated_at"),
            rs.getString("deleted_at"),
            rs.getLong("date"),
            rs.getInt("player_id"),
            rs.getString("player_name"),
            rs.getInt("seat"),
            rs.getInt("roleId"),
            rs.getInt("choose"),
            rs.getInt("power_wolf"),
            rs.getInt("operation_id"),
            rs.getInt("day"),
            rs.getInt("season_id"),
            rs.getInt("group"),
            rs.getDouble("score"),
            rs.getInt("points"),
            rs.getInt("egg"),
            rs.getInt("active_score"),
            rs.getInt("total"),
            rs.getBoolean("suicide"),
            rs.getBoolean("bottom"),
            rs.getInt("topVote"),
            rs.getInt("record_id"),
            rs.getInt("season_type_id"),
            rs.getInt("deaths_id"),
            rs.getInt("round")
          ))
        }
        Thread.sleep(5000)
//      }
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
    }
  }

}
