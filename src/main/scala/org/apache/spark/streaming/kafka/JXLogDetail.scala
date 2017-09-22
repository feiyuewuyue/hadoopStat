package org.apache.spark.streaming.kafka
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import com.sinobbd.kafka.process.StreamProcess

import kafka.serializer.StringDecoder
import com.sinobbd.kafka.process.BasicSettingReader
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat
import org.apache.spark.HashPartitioner
import com.sinobbd.kafka.process.FieldConst
import java.lang.Long
import kafka.serializer.DefaultDecoder
import scala.collection.mutable.Map
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark._
import scala.collection.mutable.HashSet


object JXLogDetail {
  val CHECKPOINT_DIR: String = BasicSettingReader.configration.get("checkpint.dir.jx").toString()
  val batch_interval = BasicSettingReader.configration.get("spark.jx.batch.interval_sec").toString()
  val kafkahosts = BasicSettingReader.configration.get("kafka.hosts.jx").toString()
  val kafkaTopics = BasicSettingReader.configration.get("kafka.topic.jx").toString()
  val kafkaGroupids = BasicSettingReader.configration.get("kafka.groupid.jx").toString()
  val esNodes = BasicSettingReader.configration.get("es.nodes.jx").toString()
  val esPort = BasicSettingReader.configration.get("es.port.jx").toString()

  def main(args1: Array[String]) {
    // args={ "test1:9092,test2:9092",}
    // System.setProperty("hadoop.home.dir", "D:\\hadoop");
    val ssc = functionToCreateContext//StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext)

    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext(): StreamingContext = {
    val args = Array(kafkahosts, kafkaTopics, kafkaGroupids) //test-consumer-group  
    val Array(brokers, topics, groupId) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("jxlogDetail")
    // sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2000")

    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.batch.size.bytes", "100mb")
    sparkConf.set("es.batch.size.entries", "10000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set("es.nodes", esNodes) //183.136.128.47:9200
    sparkConf.set("es.port", esPort)
    sparkConf.set("spark.streaming.stopGeacefullyOnShutdown", "true")
    //    sparkConf.set("spark.task.maxFailures", "1")
    //    sparkConf.set("spark.speculation", "false")
    //    sparkConf.set("spark.streaming.backpressure.enabled", "true")

    //    sparkConf.set("spark.broadcast.compress", "false");
    //    sparkConf.set("spark.shuffle.compress", "false");
    //    sparkConf.set("spark.shuffle.spill.compress", "false");

    val ssc = new StreamingContext(sparkConf, Seconds(Long.valueOf(batch_interval)))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "largest") //smallest

    val km = new KafkaManager(kafkaParams)

    val message = km.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)
    /*
 * 日志全量保留ES部分：
    1.layer:0
    2.layer:1
    3.域名中包含"wasu"的访问日志  
    4.非200、206、304以外的其他状态码日志
 */
    message.foreachRDD(rddstr => {
      if (!rddstr.isEmpty()) {
        val offsetRanges = rddstr.asInstanceOf[HasOffsetRanges].offsetRanges
        val rddAllLine = rddstr.map(f => StreamProcess.parseJXKafkaBytesForDetail(f._2))
        val rddline = rddAllLine.filter(f => {
          val status = f.get("http_status").getOrElse("0").toString()
          val domain = f.get("domain").getOrElse("0").toString()
          val layer = f.get("layer").getOrElse("0").toString()
          var isneed = true
          if ("200".equals(status) || "206".equals(status) || "304".equals(status)) {
            if (("0".equals(layer) || "1".equals(layer)) || (domain.contains("wasu"))){
              isneed = true
            }else{
              isneed = false
            }
          } else {
            isneed = true
          }

          isneed

        })
        val rddK = rddline.mapPartitions(f => { StreamProcess.getJXDetailLog(f) }, true)
        if (rddK != null && rddK.partitions.length > 0) {
          rddK.saveToEs("{index_name}/{index_type}", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "index_name,index_type"))
          km.updateZKOffsets(offsetRanges)

        }
      }

    })

    ssc
  }
}