package org.apache.spark.streaming.kafka

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import com.sinobbd.kafka.process.StreamProcess

import kafka.serializer.StringDecoder
import com.sinobbd.kafka.process.BasicSettingReader
import java.lang.Long

object StartKafkaStreamRD {
  val CHECKPOINT_DIR: String = BasicSettingReader.configration.get("checkpint.dir.rd").toString();
  val batch_interval=BasicSettingReader.configration.get("spark.fcdn.batch.interval_sec").toString()
  val kafkahostsRDs=BasicSettingReader.configration.get("kafka.hosts.rd").toString()
   val kafkaTopicRDs=BasicSettingReader.configration.get("kafka.topic.rd").toString()
   val kafkaGroupids=BasicSettingReader.configration.get("kafka.groupid.rd").toString()
  
  def main(args1: Array[String]) {
    // args={ "test1:9092,test2:9092",}
//    System.setProperty("hadoop.home.dir", "D:\\hadoop");  
//    val args = Array(kafkahostsRDs,kafkaTopicRDs,kafkaGroupids) //test-consumer-group
   
    val ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext(): StreamingContext = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val args = Array(kafkahostsRDs,kafkaTopicRDs, kafkaGroupids) //test-consumer-group
    val Array(brokers, topics, groupId) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaRDLogAnalyze")
//    sparkConf.setMaster("local[*]")    
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")   
    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
    //    sparkConf.set("spark.broadcast.compress", "false");
    //    sparkConf.set("spark.shuffle.compress", "false");
    //    sparkConf.set("spark.shuffle.spill.compress", "false");

    val ssc = new StreamingContext(sparkConf, Seconds(Long.valueOf(batch_interval)))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest")

    val km = new KafkaManager(kafkaParams)

    val message = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    var offsetRanges = Array[OffsetRange]()
    val lines = message.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)

   // val messagesMap = lines.map { value => (StreamProcess.parseJSON2Map(value)) }

//    messagesMap.foreachRDD(rdd => {
//      if (!rdd.isEmpty()) {
//        rdd.foreachPartition { f => StreamProcess.persistDetailLog(f) }
//        // 再更新offsets
//        km.updateZKOffsets(offsetRanges)
//      }
//    })
//
//    val rddK = messagesMap.map { f => StreamProcess.transToTuple1m(f) }
//    val rddRes1m = rddK.reduceByKeyAndWindow((valueMap0, valueMap1) => StreamProcess.mergeDataStatus(valueMap0, valueMap1), Seconds.apply(60), Seconds.apply(60))
//    rddRes1m.cache()
//    rddRes1m.foreachRDD(rdd1m => {
//      if (!rdd1m.isEmpty()) {
//        rdd1m.foreachPartition { f => StreamProcess.writeToES_stat(f, "statsdata_mhy_test", "bandwidth") }
//      }
//    })
//
//    val rddk5m = rddRes1m.map { f => StreamProcess.transToTuple5m(f) }
//    val rddRes5m = rddk5m.reduceByKeyAndWindow((valueMap0, valueMap1) => StreamProcess.mergeDataStatus(valueMap0, valueMap1), Seconds.apply(300), Seconds.apply(300))
//    rddRes5m.cache()
//    rddRes5m.foreachRDD(rdd5m => {
//      if (!rdd5m.isEmpty()) {
//       
//        rdd5m.foreachPartition { f => StreamProcess.writeToES_stat(f, "statsdata_5m_test", "bandwidth") }
//      }
//    })
    ssc

  }
}