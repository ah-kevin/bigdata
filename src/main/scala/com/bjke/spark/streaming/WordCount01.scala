package com.bjke.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5)) // 每隔5s划分一个批次

    ssc.checkpoint("./ckp")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9527, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val updateFunc = (currentValues: Seq[Int], historyValue: Option[Int]) => {
      if (currentValues.size > 0) {
        val currentResult = currentValues.sum + historyValue.getOrElse(0)
        Some(currentResult)
      } else {
        historyValue
      }
    }
    val wordCounts = words.map(x => (x, 1))
      //      .reduceByKey(_ + _)\
      .updateStateByKey(updateFunc)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination() // 注意： 流式应用启动之后需要一直运行等待停止/等待数据到来

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
