package com.bjke.core

import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc 需求:对SougouSearchLog进行分词并统计如下指标:
 * 1.热门搜索词
 * 2.用户热门搜索词(带上用户id)
 * 3.各个时间段搜索热度
 */
object SougouDemo {
  def main(args: Array[String]): Unit = {
    //0。准备环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //1。加载数据
    val lines = sc.textFile("data/input/SogouQ.sample")
    //2。处理数据
    val sougouRecordRDD: RDD[SogouRecord] = lines.map(line => { // map 一个进去一个出来
      val arr: Array[String] = line.split("\\s+")
      SogouRecord(
        arr(0),
        arr(1),
        arr(2),
        arr(3).toInt,
        arr(4).toInt,
        arr(5)
      )
    })
    // 切割数据

    val wordsRDD: RDD[String] = sougouRecordRDD.flatMap(record => { //flatMap是一个进去,多个出去(出去之后会被压扁) //360安全卫士==>[360, 安全卫士]
      val wordsStr: String = record.queryWords.replaceAll("\\[|\\]", "") //360安全卫士
      import scala.collection.JavaConverters._ //将Java集合转为scala集合
      HanLP.segment(wordsStr).asScala.map(_.word) //ArrayBuffer(360, 安全卫士)
    })

    //3。统计指标
    //1. 热门搜索词
    val result1: Array[(String, Int)] = wordsRDD.map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)

    //4。输出结果
    //5。释放资源
  }
  //准备一个样例类用来封装数据

  /**
   * 用户搜索点击网页记录Record
   *
   * @param queryTime  访问时间，格式为：HH:mm:ss
   * @param userId     用户ID
   * @param queryWords 查询词
   * @param resultRank 该URL在返回结果中的排名
   * @param clickRank  用户点击的顺序号
   * @param clickUrl   用户点击的URL
   */
  case class SogouRecord(
                          queryTime: String,
                          userId: String,
                          queryWords: String,
                          resultRank: Int,
                          clickRank: Int,
                          clickUrl: String
                        )
}
