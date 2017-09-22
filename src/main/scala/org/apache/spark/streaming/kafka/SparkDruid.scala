package org.apache.spark.streaming.kafka

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import kafka.serializer.StringDecoder
import org.apache.commons.codec.digest.DigestUtils
import org.joda.time.format.DateTimeFormat
import com.fasterxml.jackson.annotation.JsonValue


case class RongheData1( firm_name:String,response_time:String,http_range:String,upstream_response_time:String,time_local:String,time_unix:String,clientip:String,method:String,domain:String,request:String,protocol:String,http_status:String,body_bytes_sent:Long,referer:String,user_agent:String,hit_status_detail:String,hit_status:String,upstream_addr:String,server_addr:String,X_Info_Fetcher:String,X_Info_ObjSize:String,X_Info_request_id:String,X_Info_MD5:String,prov:String,country:String,city:String,ISP:String,latitude:String,longitude:String,blockid:String,referer_host:String, search_engine:String,hackerid:String,layer:String, def1:String,file_name:String, line_num:String,id:String,filetime:String,lowfirmname:String,rawdata:String,except_desc:String){
  
  
  @JsonValue
  def toMap: Map[String, Any] = Map(
    "firm_name" -> firm_name,
    "response_time" -> response_time,
    "domain" -> domain,
    "body_bytes_sent" -> body_bytes_sent,
    "time_local" -> time_local
  )

  def toNestedMap: Map[String, Any] = Map(
     "firm_name" -> firm_name,
    "response_time" -> response_time,
    "domain" -> domain,
    "body_bytes_sent" -> body_bytes_sent,
    "time_local" -> time_local
  )

  def toCsv: String = Seq(firm_name, response_time, domain, body_bytes_sent, time_local).mkString(",")

  
  
  
  
}

object RongheData1{
  
}
 

object SparkDruid {
  
   val kafkaParam = Map[String, String](
    "metadata.broker.list" -> "183.131.54.162:9092,183.131.54.163:9092,183.131.54.164:9092",
    "group.id" -> "druid06291",
    "auto.offset.reset" -> "largest"
  )

    def converToRonghe(line:String,token:String):RongheData1={
    var attributes=line.split(token)
  //  println(attributes(35).split("_")(2).substring(0, 8))
    println(DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z").parseDateTime(attributes(4)).toString())
    if(attributes(35).split("_")(2).substring(0, 6).equals("201705")){
      
    
    return RongheData1(attributes(0).toString(),attributes(1).toString(),attributes(2).toString(),attributes(3).toString(),attributes(4).toString(),attributes(5).toString(),
          attributes(6).toString(),attributes(7).toString(),attributes(8).toString(),attributes(9).toString(),attributes(10).toString(),attributes(11).toString(),attributes(12).toString().toLong,attributes(13).toString(),attributes(14).toString(),attributes(15).toString(),attributes(16).toString(),attributes(17).toString(),
          attributes(18).toString(),attributes(19).toString(),attributes(20).toString(),attributes(21).toString(),attributes(22).toString(),attributes(23).toString(),attributes(24).toString(),attributes(25).toString(),attributes(26).toString(),
          attributes(27).toString(),attributes(28).toString(),attributes(29).toString(),attributes(30).toString(),attributes(31).toString(),attributes(32).toString(),
          attributes(33).toString(),attributes(34).toString(),attributes(35).toString(),attributes(36).toString(),DigestUtils.md5Hex(attributes(35)+"-"+attributes(36)),attributes(35).split("_")(2).substring(0, 8)+"-1",attributes(0).toLowerCase(),"","")
    }else{
      return RongheData1(attributes(0).toString(),attributes(1).toString(),attributes(2).toString(),attributes(3).toString(),attributes(4).toString(),attributes(5).toString(),
          attributes(6).toString(),attributes(7).toString(),attributes(8).toString(),attributes(9).toString(),attributes(10).toString(),attributes(11).toString(),attributes(12).toString().toLong,attributes(13).toString(),attributes(14).toString(),attributes(15).toString(),attributes(16).toString(),attributes(17).toString(),
          attributes(18).toString(),attributes(19).toString(),attributes(20).toString(),attributes(21).toString(),attributes(22).toString(),attributes(23).toString(),attributes(24).toString(),attributes(25).toString(),attributes(26).toString(),
          attributes(27).toString(),attributes(28).toString(),attributes(29).toString(),attributes(30).toString(),attributes(31).toString(),attributes(32).toString(),
          attributes(33).toString(),attributes(34).toString(),attributes(35).toString(),attributes(36).toString(),DigestUtils.md5Hex(attributes(35)+"-"+attributes(36)),attributes(35).split("_")(2).substring(0, 8),attributes(0).toLowerCase(),"","")
    }
  }
  
  def main(args: Array[String]): Unit = {
     
     val sparkConf=new SparkConf().setAppName("SparkDruid")
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.task.maxFailures","1")
    sparkConf.set("spark.speculation","false")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","2000")
     sparkConf.set("spark.sql.warehouse.dir","file:///")
    sparkConf.set("spark.debug.maxToStringFields", "10000")
    sparkConf.set("spark.driver.allowMultipleContexts", "true");
    sparkConf.set("spark.streaming.blockInterval","100ms")
    sparkConf.set("spark.default.parallelism","300")   //executor number * executor core * 3
    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    val sparkContext = new SparkContext(sparkConf)
   
    val ssc = new StreamingContext(sparkContext, Seconds(10))
    val topic: String = "firm_logs" //消费的 topic 名字
    val topics: Set[String] = Set(topic) //创建 stream 时使用的 topic 名字集合

    var kafkaStream: InputDStream[(String, String)] = null

    kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topics)

//    kafkaStream.foreachRDD(rdd => rdd.propagate(new SimpleEventBeamFactory))
    
  val dstream= kafkaStream.map(x=>converToRonghe(x._2, "@##@"))
    import com.metamx.tranquility.spark.BeamRDD._

// Now given a Spark DStream, you can send events to Druid.
   // val logging1=new com.metamx.common.scala.Logging
    

    dstream.foreachRDD(rdd => rdd.propagate(new SimpleEventBeamFactory))/* { rdd =>
      
    
      
      
      
    //  rdd.foreach(println)
    //  rdd.foreach(strJson => Https.post("http://183.131.54.181:8200/v1/post/metricszijian9", strJson))
      
      
// Now given a Spark DStream, you can send events to Druid.
     import com.metamx.tranquility.spark.BeamRDD._
      rdd.propagate(new SimpleEventBeamFactory)
    }*/

    ssc.start()
    ssc.awaitTermination()
  }
}