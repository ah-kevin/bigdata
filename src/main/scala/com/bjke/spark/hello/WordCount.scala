package com.bjke.spark.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark-demo wordCount - 修改代码yarn集群
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("请指定input和output")
      System.exit(1) // 非0表示非正常退出程序
    }
    //1. env
    val conf = new SparkConf().setAppName("wc");
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //2. 读取数据 Rdd 弹性分布式数据集
    val lines: RDD[String] = sc.textFile(args(0))

    //3. 数据操作
    //切割Rdd[一个个单词]
    val words: RDD[String] = lines.flatMap(_.split(" "))
    var wordAndOnes: RDD[(String, Int)] = words.map((_, 1))
    // 分组聚合： groupBy+mapValues(_.map(_._2).reduce(_+_))==>spark分组+聚合一步搞定：reducerByKey
    val result: RDD[(String, Int)] = wordAndOnes.reduceByKey(_ + _)

    // sink/输出
    // 直接输出
    //    result.foreach(println);
    // 收集到本地集合在输出
    //    println(result.collect().toBuffer)
    System.setProperty("HADOOP_USER_NAME", "root")
    result.repartition(1).saveAsTextFile(args(1))
    Thread.sleep(1000 * 60)
    sc.stop();
  }
}
