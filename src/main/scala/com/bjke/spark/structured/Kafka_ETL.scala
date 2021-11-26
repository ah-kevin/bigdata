package com.bjke.spark.structured

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Kafka_ETL {
  def main(args: Array[String]): Unit = {
    // 因为StructuredStreaming基于SparkSQL的且编程API/数据抽象是DataFrame/DataSet,所以这里创建SparkSession即可
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") //本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //TODO 1.加载数据-kafka-stationTopic
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark_kafka")
      .load()
    val valueDS: Dataset[String] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    //TODO 2.处理数据-ETL-过滤出success的数据
    val etlResult: Dataset[String] = valueDS.filter(_.contains("success"))

    //TODO 3.输出结果-kafka-etlTopic
    etlResult.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "etlTopic")
      .option("checkpointLocation", "./ckp")
      //TODO 4.启动并等待结束
      .start()
      .awaitTermination()

    //TODO 5.关闭资源
    spark.stop()
  }
}
