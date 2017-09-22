package org.apache.spark.streaming.kafka

import java.lang.Long

import scala.collection.mutable.Map

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.elasticsearch.spark._

import com.sinobbd.kafka.process.BasicSettingReader
import com.sinobbd.kafka.process.StreamProcess

import kafka.serializer.DefaultDecoder
import kafka.serializer.StringDecoder

object fusionDetail {
  val CHECKPOINT_DIR: String = BasicSettingReader.configration.get("checkpint.dir.fusion").toString()
  val batch_interval = BasicSettingReader.configration.get("spark.fcdn.batch.interval_sec").toString()
  val kafkahosts = BasicSettingReader.configration.get("kafka.hosts.fcdn").toString()
  val kafkaTopics = BasicSettingReader.configration.get("kafka.topic.fcdn").toString()
  val kafkaGroupids = BasicSettingReader.configration.get("kafka.groupid.fcdn").toString()
  val esNodes= BasicSettingReader.configration.get("es.nodes.fusion").toString()
  val esPort= BasicSettingReader.configration.get("es.port.fusion").toString()

  def main(args1: Array[String]) {
    // args={ "test1:9092,test2:9092",}
   //System.setProperty("hadoop.home.dir", "D:\\hadoop");
    val ssc = functionToCreateContext//StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext)

    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext(): StreamingContext = {
    val args = Array(kafkahosts, kafkaTopics, kafkaGroupids) //test-consumer-group  
    val Array(brokers, topics, groupId) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("fusionDetail")
   // sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //从consumer offsets到leader latest offsets中间延迟了很多消息，在下一次启动的时候，首个batch要处理大量的消息，
    //会导致spark-submit设置的资源无法满足大量消息的处理而导致崩溃。因此在spark-submit启动的时候多加了一个配置:http://183.131.54.181:9100/
    //--conf spark.streaming.kafka.maxRatePerPartition=10000。限制每秒钟从topic的每个partition最多消费的消息条数，
    //这样就把首个batch的大量的消息拆分到多个batch中去了，为了更快的消化掉delay的消息，可以调大计算资源和把这个参数调大。
  

    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.batch.size.bytes", "100mb")
    sparkConf.set("es.batch.size.entries", "10000")
    sparkConf.set("es.scroll.size", "10000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set("es.nodes", esNodes) //183.136.128.47:9200 183.131.54.181
    sparkConf.set("es.port", esPort)
    //    sparkConf.set("spark.task.maxFailures", "1")
    //    sparkConf.set("spark.speculation", "false")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1500")
    sparkConf.set("spark.streaming.receiver.maxRate","3000")
    sparkConf.set("spark.streaming.stopGeacefullyOnShutdown","true")

    //    sparkConf.set("spark.broadcast.compress", "false");
    //    sparkConf.set("spark.shuffle.compress", "false");
    //    sparkConf.set("spark.shuffle.spill.compress", "false");

    val ssc = new StreamingContext(sparkConf, Seconds(Long.valueOf(batch_interval)))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest")//largest//smallest

    val km = new KafkaManager(kafkaParams)

    val message = km.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    message.foreachRDD(rddstr => {
      if (!rddstr.isEmpty()) {
        val offsetRanges = rddstr.asInstanceOf[HasOffsetRanges].offsetRanges
        val rddline = rddstr.map(f => StreamProcess.parseFusionKafkaBytesForDetail(f._2))
        val rddK = rddline.mapPartitions(f => { StreamProcess.getFusionDetailLog(f) }, true)
        if (rddK != null && rddK.partitions.length > 0) {
          rddK.count()
          rddK.saveToEs("{index_name}/{index_type}", Map("es.mapping.id" -> "id","es.mapping.exclude" -> "index_name,index_type"))
          km.updateZKOffsets(offsetRanges)

        }
      }

    })
  

    ssc
  }

}