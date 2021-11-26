package com.bjke.spark.structured

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Source_Socket {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9527)
      .load()
    df.printSchema()
    //    df.show()

    val ds: Dataset[String] = df.as[String]
    val result: Dataset[Row] = ds.flatMap(_.split(" "))
      .groupBy("value")
      .count()
      .orderBy('count.desc)

    result.writeStream.format("console").outputMode("complete").start().awaitTermination()
    spark.stop()
  }
}
