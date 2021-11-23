package com.bjke.spark.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamKafka_Demo01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val scc: StreamingContext = new StreamingContext(sc, Seconds(5))
    scc.checkpoint("./ckp")


    // 1。 加载数据-kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sparkDemo", // 消费者组名称
      "auto.offset.reset" -> "latest",
      //earliest:表示如果有offset记录从offset记录开始消费,如果没有从最早的消息开始消费
      //latest:表示如果有offset记录从offset记录开始消费,如果没有从最后/最新的消息开始消费
      //none:表示如果有offset记录从offset记录开始消费,如果没有就报错
      "auto.commit.interval.ms" -> "1000", //自动提交的时间间隔
      "enable.auto.commit" -> (true: java.lang.Boolean) //是否自动提交
    )

    val topics = Array("spark_kafka")
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      scc,
      LocationStrategies.PreferConsistent, //位置策略,使用源码中推荐的
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams) //霞飞策略,使用源码中推荐的
    )

    //2. 处理消息
    val infoDS: DStream[String] = kafkaDS.map((record: ConsumerRecord[String, String]) => {
      val topic: String = record.topic()
      val partition: Int = record.partition()
      val offset: Long = record.offset()
      val key: String = record.key()
      val value: String = record.value()
      val info: String = s"""topic:${topic}, partition:${partition}, offset:${offset}, key:${key}, value:${value}"""
      info
    })

    //3. 输出
    infoDS.print()

    // 4.启动并等待结束
    scc.start()
    scc.awaitTermination()//注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来

    // 5.关闭资源
    scc.stop(stopSparkContext = true, stopGracefully = true)//优雅关闭

  }
}
