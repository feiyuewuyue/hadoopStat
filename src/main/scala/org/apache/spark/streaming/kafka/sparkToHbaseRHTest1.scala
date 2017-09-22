package org.apache.spark.streaming.kafka

import scala.collection.mutable.Map

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.HConstants
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import java.sql.Timestamp
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import java.text.SimpleDateFormat
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat


/**
 * @author zrk
 *
 */
object sparkToHbaseRHTest1 extends Serializable {
  @transient lazy val log = LogManager.getRootLogger
  def functionToCreateContext(args: Array[String]): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("sparkToHbaseRHTest1")
      .set("spark.local.dir", "~/tmp")
      
      
      sparkConf.set("spark.task.maxFailures","1")
    sparkConf.set("spark.speculation","false")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition",args(1))
     sparkConf.set("spark.sql.warehouse.dir","file:///")
    sparkConf.set("spark.debug.maxToStringFields", "10000")
    sparkConf.set("spark.driver.allowMultipleContexts", "true");
    sparkConf.set("spark.streaming.blockInterval","100ms")
    sparkConf.set("spark.default.parallelism","300")  
    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    val ssc = new StreamingContext(sparkConf, Seconds(args(2).toLong))
 val hbaseTablePrefix = args(0)
    var table=args(0)
     val hConf = HBaseConfiguration.create()
    val zookeeper = "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181";
    hConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)

    hConf.set("hbase.zookeeper.quorum", "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181");
	    hConf.set("hbase.rpc.timeout", "1800000");
	    hConf.set("mapreduce.task.timeout", "1800000");
	      hConf.set("hbase.defaults.for.version.skip", "true")
    val jobConf = new JobConf(hConf, this.getClass)
    // Create direct kafka stream with brokers and topics
    val topicsSet = args(3).split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> "183.131.54.162:2181,183.131.54.163:9092", "auto.offset.reset" -> "largest", "group.id" -> args(4),"auto.commit.enable"->"false")
    val km = new KafkaManager1(kafkaParams)
    val kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    log.warn(s"Initial Done***>>>")

    kafkaStream.cache

    //do something......
    
    val chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    
    val appUserClicks = kafkaStream.flatMap(rdd => {
      val data = rdd._2.split("@##@", -1)
      Some(data)
    }).map{pair =>
      var id=""
      var value=""
      var value1=""
      if (pair.length == 37) {
       val tmpDate=getDateString(pair(5))
       id= chars.charAt((pair(36).toLong%36).toInt)+"#!"+tmpDate+"#!"+pair(0)+"#"+DigestUtils.md5Hex(pair(35)) +"#"+ pair(36)
            
            value=("\""+pair(6)+"\" \""+pair(1)+"\" \""+pair(4)
			+"\" \""+pair(7)+"\" \""+pair(8)+"\" \""+pair(9)+"\" \""+pair(10)+"\" \""
			+pair(11)+"\" \""+pair(12)+"\" \""+pair(13)+"\" \""+pair(14)+
			"\" \""+pair(16)+"\"");
           
          value1=("\""+pair(0)+"\" \""+pair(2)+"\" \""+pair(3)
			+"\" \""+pair(5)+"\" \""+pair(15)+"\" \""+pair(17)+"\" \""+pair(18)+"\" \""
			+pair(19)+"\" \""+pair(20)+"\" \""+pair(21)+"\" \""+pair(22)+
			"\" \""+pair(23)+"\" \""+pair(24)+"\" \""+pair(25)+"\" \""+pair(26)+"\" \""
			+pair(27)+"\" \""+pair(28)+"\" \""+pair(29)+"\" \""+pair(30)+
			"\" \""+pair(31)+"\" \""+pair(32)+"\" \""+pair(33)+"\" \""+pair(34)+"\" \""+pair(35)+"\" \""+pair(36)+"\"");
         if(compareDate(pair(5))){
				table=(hbaseTablePrefix+tmpDate.substring(0,8));
			}else{
				
				table=(hbaseTablePrefix+tmpDate.substring(0,6));
			}
      }else{
        table=hbaseTablePrefix;
			id = pair(36).toLong%10+"#!"+"199700000000"+"#"+DigestUtils.md5Hex(pair(35)) +"#"+ pair(36)
			value1="\""+pair(37)+"\" \""+pair(38);
      }
        
     
        (id, value,value1)
    }

    val result = appUserClicks.map { item =>
      val rowKey = item._1
      val value = item._2
      val value1=item._3
      convertToHbasePut(rowKey, value,value1, table)
    }

    result.foreachRDD { rdd =>
      
      rdd.saveAsNewAPIHadoopFile("", classOf[ImmutableBytesWritable], classOf[Put], classOf[MultiTableOutputFormat], jobConf)
       
    }

    
    
    
    

    //更新zk中的offset
    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        km.updateZKOffsets(rdd)
    })

    ssc
  }

   def convertToHbasePut(key: String, value: String,value1:String, tableName: String): (ImmutableBytesWritable, Put) = {
    val rowKey = key
    val ts=System.currentTimeMillis();
    val put = new Put(Bytes.toBytes(rowKey),ts)
    if(!value.equals("")){
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info"), Bytes.toBytes(value))
    }
     put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info1"), Bytes.toBytes(value1))
    // put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info1"), ts,Bytes.toBytes(value1));
    (new ImmutableBytesWritable(Bytes.toBytes(tableName)), put)
  }
  
  def compareDate(timeStamp:String):(Boolean)={
		val nowTime=System.currentTimeMillis()/1000;
		val logTime=timeStamp.toLong;
		val dataTime=(logTime - 57600)/86400*86400 + 57600;
		
		if(nowTime-dataTime<=30*24*60*60){  //1491580740    1491321540
			return true;
		}
		return false;
	}
  
  	def getDateString(tenLength:String):(String)={
		
		var tmp=tenLength.toLong;
		tmp=tmp/60*60;
    	val ts = new Timestamp(tmp*1000);
        var tsStr = "";  
        var sdf = new SimpleDateFormat("yyyyMMddHHmm");
        try {  
            //方法一  
            tsStr = sdf.format(ts);
        } catch{  
          case e:Exception=>e.printStackTrace()  
        }
        return tsStr;
	}
  

  def main(args: Array[String]) {
    val ssc = functionToCreateContext(args)
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}