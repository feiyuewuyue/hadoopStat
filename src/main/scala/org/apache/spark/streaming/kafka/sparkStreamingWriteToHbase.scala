package org.apache.spark.streaming.kafka

import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.hbase.HConstants
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.streaming.Seconds
import net.sf.json.JSONObject
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat
import java.text.DateFormat
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.commons.codec.digest.DigestUtils

object sparkStreamingWriteToHbase {
  def main(args: Array[String]): Unit = {
 /*   var masterUrl = "yarn-client"
    if (args.length > 0) {
      masterUrl = args(0)
    }*/
    val conf = new SparkConf().setAppName("Write to several tables of Hbase")

           conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.task.maxFailures","1")
    conf.set("spark.speculation","false")
    conf.set("spark.streaming.kafka.maxRatePerPartition",args(1))
     conf.set("spark.sql.warehouse.dir","file:///")
    conf.set("spark.debug.maxToStringFields", "10000")
    conf.set("spark.driver.allowMultipleContexts", "true");
    conf.set("spark.streaming.blockInterval","100ms")
    conf.set("spark.default.parallelism","300")  
    conf.set("spark.executor.userClassPathFirst", "true");
    conf.set("spark.driver.userClassPathFirst", "true");
    
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))
    var table=args(0)
    val topics = Set("firm_logs")

    val brokers = "183.131.54.162:2181,183.131.54.163:9092"

    val zkHost="183.131.54.162:2181,183.131.54.163:2181,183.131.54.164:2181";
    
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,"zookeeper.connect"->zkHost,"group.id"->"firm_logszrkspark", "auto.offset.reset"->"largest","auto.commit.enable"->"false","serializer.class" -> "kafka.serializer.StringEncoder")

    val hbaseTablePrefix = "test"

    val hConf = HBaseConfiguration.create()
    val zookeeper = "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181";
    hConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)

    hConf.set("hbase.zookeeper.quorum", "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181");
	    hConf.set("hbase.rpc.timeout", "1800000");
	    hConf.set("mapreduce.task.timeout", "1800000");
	      hConf.set("hbase.defaults.for.version.skip", "true")
    val jobConf = new JobConf(hConf, this.getClass)

    val kafkaDStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val appUserClicks = kafkaDStreams.flatMap(rdd => {
      val data = rdd._2.split("@##@", -1)
      Some(data)
    }).map{pair =>
       val tmpDate=getDateString(pair(5))
       val id= pair(36).toLong%10+"#!"+tmpDate+"#!"+pair(0)+"#"+DigestUtils.md5Hex(pair(35)) +"#"+ pair(36)
            
           val value=("\""+pair(6)+"\" \""+pair(1)+"\" \""+pair(4)
			+"\" \""+pair(7)+"\" \""+pair(8)+"\" \""+pair(9)+"\" \""+pair(10)+"\" \""
			+pair(11)+"\" \""+pair(12)+"\" \""+pair(13)+"\" \""+pair(14)+
			"\" \""+pair(16)+"\"");
           
            val value1=("\""+pair(0)+"\" \""+pair(2)+"\" \""+pair(3)
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

    ssc.start()
    ssc.awaitTermination()
  }

  def convertToHbasePut(key: String, value: String,value1:String, tableName: String): (ImmutableBytesWritable, Put) = {
    val rowKey = key
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info"), Bytes.toBytes(value))
     put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info1"), Bytes.toBytes(value1))
    (new ImmutableBytesWritable(Bytes.toBytes(tableName)), put)
  }
  
  def compareDate(timeStamp:String):(Boolean)={
		val nowTime=System.currentTimeMillis()/1000;
		val logTime=timeStamp.toLong;
		val dataTime=(logTime - 57600)/86400*86400 + 57600;
		
		if(nowTime-dataTime<=3*24*60*60){  //1491580740    1491321540
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
  
  
  

}