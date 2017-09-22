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

//case class RongheData( firm_name:String,response_time:String,http_range:String,upstream_response_time:String,time_local:String,time_unix:String,clientip:String,method:String,domain:String,request:String,protocol:String,http_status:String,body_bytes_sent:Long,referer:String,user_agent:String,hit_status_detail:String,hit_status:String,upstream_addr:String,server_addr:String,X_Info_Fetcher:String,X_Info_ObjSize:String,X_Info_request_id:String,X_Info_MD5:String,prov:String,country:String,city:String,ISP:String,latitude:String,longitude:String,blockid:String,referer_host:String, search_engine:String,hackerid:String,layer:String, def1:String,file_name:String, line_num:String,id:String,filetime:String)
 
object sparkToESRH {
  
  
  
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
    
    if(attributes(35).split("_")(2).substring(0, 6).equals("201705")){
      
    if(attributes.length==40){
    return RongheData(attributes(0).toString(),attributes(1).toString(),attributes(2).toString(),attributes(3).toString(),attributes(4).toString(),attributes(5).toString(),
          attributes(6).toString(),attributes(7).toString(),attributes(8).toString(),attributes(9).toString(),attributes(10).toString(),attributes(11).toString(),attributes(12).toString().toLong,attributes(13).toString(),attributes(14).toString(),attributes(15).toString(),attributes(16).toString(),attributes(17).toString(),
          attributes(18).toString(),attributes(19).toString(),attributes(20).toString(),attributes(21).toString(),attributes(22).toString(),attributes(23).toString(),attributes(24).toString(),attributes(25).toString(),attributes(26).toString(),
          attributes(27).toString(),attributes(28).toString(),attributes(29).toString(),attributes(30).toString(),attributes(31).toString(),attributes(32).toString(),
          attributes(33).toString(),attributes(34).toString(),attributes(35).toString(),attributes(36).toString(),DigestUtils.md5Hex(attributes(35)+"-"+attributes(36)),attributes(35).split("_")(2).substring(0, 8)+"-1",attributes(0).toLowerCase(),attributes(38).toString(),attributes(39).toString())
    }else{
      return RongheData(attributes(0).toString(),attributes(1).toString(),attributes(2).toString(),attributes(3).toString(),attributes(4).toString(),attributes(5).toString(),
          attributes(6).toString(),attributes(7).toString(),attributes(8).toString(),attributes(9).toString(),attributes(10).toString(),attributes(11).toString(),attributes(12).toString().toLong,attributes(13).toString(),attributes(14).toString(),attributes(15).toString(),attributes(16).toString(),attributes(17).toString(),
          attributes(18).toString(),attributes(19).toString(),attributes(20).toString(),attributes(21).toString(),attributes(22).toString(),attributes(23).toString(),attributes(24).toString(),attributes(25).toString(),attributes(26).toString(),
          attributes(27).toString(),attributes(28).toString(),attributes(29).toString(),attributes(30).toString(),attributes(31).toString(),attributes(32).toString(),
          attributes(33).toString(),attributes(34).toString(),attributes(35).toString(),attributes(36).toString(),DigestUtils.md5Hex(attributes(35)+"-"+attributes(36)),attributes(35).split("_")(2).substring(0, 8)+"-1",attributes(0).toLowerCase(),"","")
    }
    }else{
      
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
  }
  def main(args: Array[String]): Unit = {
    
    
    
    val zkHost=args(0);
    val brokerList=args(1)
    val zkClient=new ZkClient(zkHost)
  //  val kafkaParams=Map[String,String]("metadata.broker.list" -> brokerList,
    //    "zookeeper.connect"->zkHost,"group.id"->(args(7)+"-1"),"auto.offset.reset"->"largest","auto.commit.enable"->"false") //largest,smallest
    var kafkaStream:InputDStream[(String,String)]=null
    var offsetRanges=Array[OffsetRange]()
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
      val kafkaParams = scala.collection.immutable.Map[String, String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> (args(7)),
      "auto.offset.reset" -> "largest","auto.commit.enable"->"false")//largest//smallest
   // sparkConf.set("es.batch.size.bytes","10m")
    val km = new KafkaManager(kafkaParams)
    
    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
       sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.batch.size.bytes", "450mb")
    sparkConf.set("es.batch.size.entries", "25000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set(" es.batch.write.retry.count","3")
  //  sparkConf.set("es.input.max.docs.per.partition","200000")
    sparkConf.set("fetch.message.max.bytes","5m")
    val ssc = new StreamingContext(sparkConf, Seconds(args(5).toLong))
    val topic=args(6)
     val topicsSet = topic.split(",").toSet
   /* val topicDirs=new ZKGroupTopicDirs(args(7), topic)
    val children=zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    var fromOffSets:Map[TopicAndPartition,Long]=Map()
    if (children>0){
      for(i <-0 until children){
        val partitionOffset=zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp=TopicAndPartition(topic,i)
        fromOffSets += (tp-> partitionOffset.toLong) 
      }
      
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())  //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffSets, messageHandler)
      
     // kafkaStream=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffSets, messageHandler)
    }else{
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
      }*/
   kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
     
      
      
      kafkaStream.foreachRDD(rdds=>{
        val sqlContext = SQLContextSingleton.getInstance(rdds.sparkContext)
        import sqlContext.implicits._
         offsetRanges = rdds.asInstanceOf[HasOffsetRanges].offsetRanges
         rdds.map(x=>converToRonghe(x._2, args(8))).filter(defFilter).toDF().saveToEs(args(11)+"{lowfirmname}-{filetime}/"+args(12),Map("es.mapping.id" ->"id","es.mapping.exclude" -> "id,filetime,lowfirmname"))
        km.updateZKOffsets(offsetRanges)
      })
 /* kafkaStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(x => x._2.split(args(8), -1)).foreachRDD(rdd=>{
      
      //rdd.foreach(s=>println(s))
     // offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      val logDataFrame = rdd.map(attributes=>RongheData(attributes(0).toLowerCase(),attributes(1).toString(),attributes(2).toString(),attributes(3).toString(),attributes(4).toString(),attributes(5).toString(),
          attributes(6).toString(),attributes(7).toString(),attributes(8).toString(),attributes(9).toString(),attributes(10).toString(),attributes(11).toString(),attributes(12).toString().toLong,attributes(13).toString(),attributes(14).toString(),attributes(15).toString(),attributes(16).toString(),attributes(17).toString(),
          attributes(18).toString(),attributes(19).toString(),attributes(20).toString(),attributes(21).toString(),attributes(22).toString(),attributes(23).toString(),attributes(24).toString(),attributes(25).toString(),attributes(26).toString(),
          attributes(27).toString(),attributes(28).toString(),attributes(29).toString(),attributes(30).toString(),attributes(31).toString(),attributes(32).toString(),
          attributes(33).toString(),attributes(34).toString(),attributes(35).toString(),attributes(36).toString(),DigestUtils.md5Hex(attributes(35)+"-"+attributes(36)),attributes(35).split("_")(2).substring(0, 8))).toDF().saveToEs(args(11)+"{firm_name}-{filetime}/"+args(12),Map("es.mapping.id" ->"id","es.mapping.exclude" -> "id,filetime"))
          
          
      //注册为tempTable  .filter(defFilter)    .repartition(args(13).toInt)
      km.updateZKOffsets(offsetRanges)
    //  logDataFrame.registerTempTable(args(9))
      
    //  val logCountsDataFrame =
   //     sqlContext.sql(args(10)).saveToEs(args(11)+"{firm_name}-{filetime}/"+args(12),Map("es.mapping.id" ->"id","es.mapping.exclude" -> "id,filetime"))
      // sqlContext.sql(args(10)).save
     //  sqlContext.sql("SELECT from_unixtime(time_unix,'yyyyMMdd') as time_unix,lower(firm_name) as firm_name, count(firm_name) FROM ronghe GROUP BY firm_name,time_unix")
      //打印查询结果
    //  logCountsDataFrame.show(10)
    
      
      
        for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
      }
      
      
      
      //-------------------------------------------------
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
      }
      
    })
 */
    ssc.start()
    ssc.awaitTermination()

    
  }



}


/*object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}*/