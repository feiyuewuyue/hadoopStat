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
object startJXBandStatis {
  //val CHECKPOINT_DIR: String = BasicSettingReader.configration.get("checkpint.dir.jx").toString()
  val batch_interval = BasicSettingReader.configration.get("spark.jx.batch.interval_sec").toString()
  val kafkahosts = BasicSettingReader.configration.get("kafka.hosts.jx").toString()
  val kafkaTopics = BasicSettingReader.configration.get("kafka.topic.jx").toString()
  val kafkaGroupids = BasicSettingReader.configration.get("kafka.groupid.jx.statis").toString()
  val esNodes = BasicSettingReader.configration.get("es.nodes.jx").toString()
  val esPort = BasicSettingReader.configration.get("es.port.jx").toString()

  def main(args1: Array[String]) {
    // args={ "test1:9092,test2:9092",}
    //  System.setProperty("hadoop.home.dir", "D:\\hadoop");
    val ssc = functionToCreateContext // StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext)

    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext(): StreamingContext = {
    val args = Array(kafkahosts, kafkaTopics, kafkaGroupids) //test-consumer-group  
    val Array(brokers, topics, groupId) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaLogAnalyze")
    //sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //从consumer offsets到leader latest offsets中间延迟了很多消息，在下一次启动的时候，首个batch要处理大量的消息，
    //会导致spark-submit设置的资源无法满足大量消息的处理而导致崩溃。因此在spark-submit启动的时候多加了一个配置:
    //--conf spark.streaming.kafka.maxRatePerPartition=10000。限制每秒钟从topic的每个partition最多消费的消息条数，
    //这样就把首个batch的大量的消息拆分到多个batch中去了，为了更快的消化掉delay的消息，可以调大计算资源和把这个参数调大。
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "3000")

    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.batch.size.bytes", "300mb")
    sparkConf.set("es.batch.size.entries", "10000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set("es.nodes", esNodes) //183.136.128.47:9200
    sparkConf.set("es.port", esPort)
    //  sparkConf.set("spark.task.maxFailures", "1")
    //  sparkConf.set("spark.speculation", "false")
    sparkConf.set("spark.streaming.stopGeacefullyOnShutdown", "true")
    //   sparkConf.set("spark.streaming.backpressure.enabled", "true")  
   
    sparkConf.set("es.batch.write.retry.count", "50") 
    sparkConf.set("es.batch.write.retry.wait", "500") 
    sparkConf.set("es.http.timeout", "5m") 
    sparkConf.set("es.http.retries", "50") 
    sparkConf.set("es.action.heart.beat.lead", "50") 

    //    sparkConf.set("spark.broadcast.compress", "false");
    //    sparkConf.set("spark.shuffle.compress", "false");
    //    sparkConf.set("spark.shuffle.spill.compress", "false");

    val ssc = new StreamingContext(sparkConf, Seconds(Long.valueOf(batch_interval)))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "largest")

    val km = new KafkaManager(kafkaParams)

    val message = km.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)
    var offsetRanges = Array[OffsetRange]()
    val lines = message.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val offset = StreamProcess.genPartitionMinOffset(offsetRanges)
      val rddnew = rdd.flatMap(f => {
        val hmDetail = StreamProcess.parseJXKafkaBytesForBandWithOffset(f._2, offset)
        val res = StreamProcess.transDetailLogToJXBand(hmDetail)
        res

      })
      rddnew
    }

    //将数据转换为key-value形式，并在同一个RDD内汇总
    //    val rddK = lines.mapPartitions(f => { StreamProcess.mergeDetailLogJX(f) }, true)

    val rddRes = lines.reduceByKeyAndWindow((valueMap0, valueMap1) => StreamProcess.mergeDataStatis(valueMap0, valueMap1), Seconds.apply(60), Seconds.apply(60))

    rddRes.foreachRDD(rddxm => {
      if (!rddxm.isEmpty()) {
        val rddSave = rddxm.map(f => { StreamProcess.setAndModifyID(f._2) })
        //        val rddSave = rddxm.map(f => f._2)       
        rddSave.saveToEs("{index_name}/{index_type}", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "id,index_name,index_type,offset"))
      }

      //一次更新60000/分区  共120个分区   10个excuter    spark 每次取10000/分区    每次Input Size 1200000 records     23个batch（23*12=27600000）     更新的offset 1380000  (165600000)
      km.updateZKOffsets(offsetRanges)
    })

    ssc
  }
}