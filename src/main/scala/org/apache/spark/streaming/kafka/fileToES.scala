package org.apache.spark.streaming.kafka

import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.elasticsearch.spark.sql.sparkDataFrameFunctions

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZKGroupTopicDirs
import kafka.utils.ZkUtils
import org.apache.commons.codec.digest.DigestUtils


object fileToES {
  
  
  
  
  def defFilter(fieldArray:RongheData):Boolean={
     if ("200".equals(fieldArray.http_status) || "206".equals(fieldArray.http_status) || "304".equals(fieldArray.http_status)) {
            if (("0".equals(fieldArray.layer) || "1".equals(fieldArray.layer)) || (fieldArray.domain.contains("wasu"))) {
                return true;
            }
            return false;
        }
        return true;
    
  }
  
  
  def converToRonghe(line:String,token:String):RongheData={
    var attributes=line.split(token)
    println(attributes(35).split("_")(2).substring(0, 8))
    

      
      if(attributes.length==40){
      return RongheData(attributes(0).toString(),attributes(1).toString(),attributes(2).toString(),attributes(3).toString(),attributes(4).toString(),attributes(5).toString(),
          attributes(6).toString(),attributes(7).toString(),attributes(8).toString(),attributes(9).toString(),attributes(10).toString(),attributes(11).toString(),attributes(12).toString().toLong,attributes(13).toString(),attributes(14).toString(),attributes(15).toString(),attributes(16).toString(),attributes(17).toString(),
          attributes(18).toString(),attributes(19).toString(),attributes(20).toString(),attributes(21).toString(),attributes(22).toString(),attributes(23).toString(),attributes(24).toString(),attributes(25).toString(),attributes(26).toString(),
          attributes(27).toString(),attributes(28).toString(),attributes(29).toString(),attributes(30).toString(),attributes(31).toString(),attributes(32).toString(),
          attributes(33).toString(),attributes(34).toString(),attributes(35).toString(),attributes(36).toString(),DigestUtils.md5Hex(attributes(35)+"-"+attributes(36)),attributes(35).split("_")(2).substring(0, 8),attributes(0).toLowerCase(),attributes(38).toString(),attributes(39).toString())
      }else{
          return RongheData(attributes(0).toString(),attributes(1).toString(),attributes(2).toString(),attributes(3).toString(),attributes(4).toString(),attributes(5).toString(),
          attributes(6).toString(),attributes(7).toString(),attributes(8).toString(),attributes(9).toString(),attributes(10).toString(),attributes(11).toString(),attributes(12).toString().toLong,attributes(13).toString(),attributes(14).toString(),attributes(15).toString(),attributes(16).toString(),attributes(17).toString(),
          attributes(18).toString(),attributes(19).toString(),attributes(20).toString(),attributes(21).toString(),attributes(22).toString(),attributes(23).toString(),attributes(24).toString(),attributes(25).toString(),attributes(26).toString(),
          attributes(27).toString(),attributes(28).toString(),attributes(29).toString(),attributes(30).toString(),attributes(31).toString(),attributes(32).toString(),
          attributes(33).toString(),attributes(34).toString(),attributes(35).toString(),attributes(36).toString(),DigestUtils.md5Hex(attributes(35)+"-"+attributes(36)),attributes(35).split("_")(2).substring(0, 8),attributes(0).toLowerCase(),"","")
      }
    
  }
  def main(args: Array[String]): Unit = {
    
    
    
   
     val sparkConf = new SparkConf().setAppName("RHEsDetail")
    //每60秒一个批次
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.task.maxFailures","1")
    sparkConf.set("spark.speculation","false")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition",args(2))
     sparkConf.set("spark.sql.warehouse.dir","file:///")
    sparkConf.set("spark.debug.maxToStringFields", "10000")
    sparkConf.set("spark.driver.allowMultipleContexts", "true");
    sparkConf.set("spark.streaming.blockInterval","100ms")
    sparkConf.set("spark.default.parallelism","300")   //executor number * executor core * 3
    sparkConf.set("es.nodes", args(3))
    .set("es.port", args(4))
    
    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
       sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.batch.size.bytes", "450mb")
    sparkConf.set("es.batch.size.entries", "25000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set(" es.batch.write.retry.count","3")
  //  sparkConf.set("es.input.max.docs.per.partition","200000")
    sparkConf.set("fetch.message.max.bytes","5m")
  val sc=new SparkContext(sparkConf)
  // kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](
    //  ssc, kafkaParams, topicsSet)
    val kafkaStream= sc.textFile("hdfs://...")
 

    val sqlContext = SQLContextSingleton.getInstance(sc)
        import sqlContext.implicits._
       kafkaStream.map(x=>converToRonghe(x, args(8))).toDF().saveToEs(args(11)+"{lowfirmname}-{filetime}/"+args(12),Map("es.mapping.id" ->"id","es.mapping.exclude" -> "id,filetime,lowfirmname"))
  //  sc.start()
   // sc.awaitTermination()

    
  }




}