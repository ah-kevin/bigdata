package com.bjke.flink.table

import com.bjke.flink.model.Order
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import java.time.Duration
import java.util.UUID
import scala.util.Random

/**
 * 演示Flink Table&SQL 案例- 从Kafka:input_kafka主题消费数据并生成Table,然后过滤出状态为success的数据再写回到Kafka:output_kafka主题
 */
object Demo4 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    tenv.executeSql(
      """
        | CREATE TABLE input_kafka (
        |  `user_id` BIGINT,
        |  `page_id` BIGINT,
        |  `status` STRING
        |  ) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'topic',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json'
        |  )
        |""".stripMargin)

    val etlResult: Table = tenv.sqlQuery("""select * from input_kafka where status='success'""")
    val value: DataStream[(Boolean, Row)] = tenv.toRetractStream[Row](etlResult)
    value.print()
    tenv.executeSql(
      """
        | CREATE TABLE output_kafka (
        |  `user_id` BIGINT,
        |  `page_id` BIGINT,
        |  `status` STRING
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'etlTopic',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json',
        |  'sink.partitioner' = 'round-robin'
        |  )
        |""".stripMargin)

    tenv.executeSql("insert into output_kafka select * from " + etlResult)

    env.execute();
  }
}
//{"user_id": "1", "page_id":"1", "status": "success"}
//{"user_id": "1", "page_id":"1", "status": "success"}
//{"user_id": "1", "page_id":"1", "status": "success"}
//{"user_id": "1", "page_id":"1", "status": "success"}
//{"user_id": "1", "page_id":"1", "status": "fail"}