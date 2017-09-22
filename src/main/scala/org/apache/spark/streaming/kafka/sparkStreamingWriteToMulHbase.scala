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
import kafka.message.MessageAndMetadata
import org.apache.spark.SparkContext
import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.InputDStream
import kafka.common.TopicAndPartition
import java.util.ArrayList
import kafka.utils.ZkUtils
import java.util.Random


object sparkStreamingWriteToMulHbase {
  
  def main(args: Array[String]): Unit = {
 /*   var masterUrl = "yarn-client"
    if (args.length > 0) {
      masterUrl = args(0)
    }*/
/*    val conf = new SparkConf().setAppName("Write to several tables of Hbase")

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
*/
         val zkHost="183.131.54.162:2181,183.131.54.163:2181,183.131.54.164:2181";
    val brokerList="183.131.54.162:2181,183.131.54.163:9092"
    val zkClient=new ZkClient(zkHost)
  //  val kafkaParams=Map[String,String]("metadata.broker.list" -> brokerList,
   //     "zookeeper.connect"->zkHost,"group.id"->"zrktestid22","auto.offset.reset"->"largest","auto.commit.enable"->"false") //largest
          val kafkaParams = scala.collection.immutable.Map[String, String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> (args(4)),
      "auto.offset.reset" -> "smallest","auto.commit.enable"->"false")//largest//smallest
      val km = new KafkaManager(kafkaParams)
    var kafkaStream:InputDStream[(String,String)]=null
    var offsetRanges=Array[OffsetRange]()
     val sparkConf = new SparkConf().setAppName("Streaming Hbase")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
    val sc=new SparkContext(sparkConf)
    val hbaseTablePrefix = "test"
    var table=args(0)
     val hConf = HBaseConfiguration.create()
    val zookeeper = "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181";
    hConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)

    hConf.set("hbase.zookeeper.quorum", "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181");
	    hConf.set("hbase.rpc.timeout", "1800000");
	    hConf.set("mapreduce.task.timeout", "1800000");
	      hConf.set("hbase.defaults.for.version.skip", "true")
    val jobConf = new JobConf(hConf, this.getClass)
    
     val ssc = new StreamingContext(sc, Seconds(args(2).toLong))
    //val topic="firm_logs"
	         val topic=args(3)
     val topicsSet = topic.split(",").toSet
    val puts=new ArrayList();
    val topicDirs=new ZKGroupTopicDirs("spark_streaming_testid22", topic)
    val children=zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    var fromOffSets:Map[TopicAndPartition,Long]=Map()
  /*  if (children>0){
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
    
       val chars = "0123456789abcdefghijklmnopqrstuvwxyz"
   // val rand=new Random(10)
 /*   kafkaStream.foreachRDD(rdds=>{
       offsetRanges = rdds.asInstanceOf[HasOffsetRanges].offsetRanges
       rdds.map(x=>converToRonghe(x._2, args(8)))
      
    })
       */
       
       
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

    kafkaStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //寰楀埌璇� rdd 瀵瑰簲 kafka 鐨勬秷鎭殑 offset
      rdd
    }.foreachRDD(rdd=>{
        km.updateZKOffsets(offsetRanges)
 
     // print("zrkcount"+rdd.count())
      //rdd.foreach(s=>println(s))
     // offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //  for (o <- offsetRanges) {
     //   val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
       // ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //灏嗚 partition 鐨� offset 淇濆瓨鍒� zookeeper
      //}
    })
    
    ssc.start()
    ssc.awaitTermination()
  }

  def convertToHbasePut(key: String, value: String,value1:String, tableName: String): (ImmutableBytesWritable, Put) = {
    val rowKey = key
    val ts=System.currentTimeMillis();
    val put = new Put(Bytes.toBytes(rowKey),ts)
    if(!value.equals("")){
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info"), Bytes.toBytes(value))
    }
     put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info1"), Bytes.toBytes(value1))
     
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
  
  
  


}