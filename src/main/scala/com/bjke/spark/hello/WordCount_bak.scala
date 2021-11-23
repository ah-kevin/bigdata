package com.bjke.spark.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark-demo wordCount
 */
object WordCount_bak {
  def main(args: Array[String]): Unit = {
    //1. env
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //2. 读取数据 Rdd 弹性分布式数据集
    val lines: RDD[String] = sc.textFile("data/input/word.txt")

    //3. 数据操作
    //切割Rdd[一个个单词]
    val words: RDD[String] = lines.flatMap(_.split(" "))
    var wordAndOnes: RDD[(String, Int)] = words.map((_, 1))
    // 分组聚合： groupBy+mapValues(_.map(_._2).reduce(_+_))==>spark分组+聚合一步搞定：reducerByKey
    val result: RDD[(String, Int)] = wordAndOnes.reduceByKey(_ + _)

    // sink/输出
    // 直接输出
    result.foreach(println);
    // 收集到本地集合在输出
    println(result.collect().toBuffer)

    //    result.repartition(1).saveAsTextFile("data/output/result.txt")
    Thread.sleep(1000 * 60)
    sc.stop();
  }
}
