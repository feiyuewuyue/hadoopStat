package org.apache.spark.streaming.kafka

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import kafka.serializer.StringDecoder

/**
  * Describe: 
  * Author: 张政
  * Data: 2016/10/13 20:03
  */
object DirectKafka {
  /**
    * 处理RDD
    * @param rdd
    */
  def processRdd(rdd: RDD[(String, String)]): Unit = {
    val lines = rdd.map(_._2)
    //    println("######################"+lines.collect().toBuffer+"---------"+rdd.map(_._1).collect().toBuffer)
    //    val words = lines.map(_.split(" ")) //org.apache.spark.SparkException: Cannot use map-side combining with array keys.
    /*val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreach(println)*/
    //183.131.54.130:9092,183.131.54.131:9092,183.131.54.132:9092 nginx-access gg
    //183.131.54.162:9092,183.131.54.163:9092,183.131.54.164:9092,183.131.54.165:9092,183.131.54.166:9092 test gg
    lines.foreach(println(_))
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics> <groupid>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <groupid> is a consume group
           |
        """.stripMargin)
      System.exit(1)
    }
 System.setProperty("hadoop.home.dir", "D:\\hadoop");
    Logger.getLogger("org").setLevel(Level.WARN)  //设置日志级别

    val Array(brokers, topics, groupId) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    sparkConf.setMaster("local[*]") //自动设置, 最大是cpu的核数
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5") //1秒拉取5条
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(sparkConf, Seconds(5)) //10秒执行一次

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,  //指定brokers的地址
      "group.id" -> groupId,  //groupid
      "auto.offset.reset" -> "smallest" //偏移量
    )

    val km = new KafkaManager(kafkaParams)

    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        processRdd(rdd)// 先处理消息
//        km.updateZKOffsets(rdd)// 再更新offsets
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
