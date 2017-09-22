package org.apache.spark.streaming.kafka

import java.util.ArrayList

import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZKGroupTopicDirs
import kafka.utils.ZkUtils
import java.util.Random



object streamingHbase2 {
  
  
  def main(args: Array[String]): Unit = {
    
    
/*      val zkHost="183.131.54.162:2181,183.131.54.163:2181,183.131.54.164:2181";
    val brokerList="183.131.54.162:2181,183.131.54.163:9092"
    val zkClient=new ZkClient(zkHost)
    val kafkaParams=Map[String,String]("metadata.broker.list" -> brokerList,
        "zookeeper.connect"->zkHost,"group.id"->"zrktestid0523","auto.offset.reset"->"largest","auto.commit.enable"->"false") //largest
    var kafkaStream:InputDStream[(String,String)]=null
    var offsetRanges=Array[OffsetRange]()
     val sparkConf = new SparkConf().setAppName("Streaming Hbase")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.task.maxFailures","1")
    sparkConf.set("spark.speculation","false")
    sparkConf.set("spark.driver.allowMultipleContexts","true")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1000")
     sparkConf.set("spark.sql.warehouse.dir","file:///")
    sparkConf.set("spark.debug.maxToStringFields", "10000")
    sparkConf.set("spark.driver.allowMultipleContexts", "true");
    sparkConf.set("spark.streaming.blockInterval","100ms")
    sparkConf.set("spark.default.parallelism","300")  
    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    val sc=new SparkContext(sparkConf)
    val cf="a"
    
     val ssc = new StreamingContext(sparkConf, Seconds(10))
    val topic="firm_logs"
    val puts=new ArrayList();
    val topicDirs=new ZKGroupTopicDirs("spark_streaming_testid0523", topic)
    val children=zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    var fromOffSets:Map[TopicAndPartition,Long]=Map()
    if (children>0){
      for(i <-0 until children){
        val partitionOffset=zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp=TopicAndPartition(topic,i)
        fromOffSets += (tp-> partitionOffset.toLong) 
      }
      
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())  //杩欎釜浼氬皢 kafka 鐨勬秷鎭繘琛� transform锛屾渶缁� kafak 鐨勬暟鎹兘浼氬彉鎴� (topic_name, message) 杩欐牱鐨� tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffSets, messageHandler)
      
     // kafkaStream=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffSets, messageHandler)
    }else{
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
      }
   */
    
     val zkHost="183.131.54.162:2181,183.131.54.163:2181,183.131.54.164:2181";
    val brokerList="183.131.54.162:2181,183.131.54.163:9092"
    val zkClient=new ZkClient(zkHost)
    val kafkaParams=Map[String,String]("metadata.broker.list" -> brokerList,
        "zookeeper.connect"->zkHost,"group.id"->"zrktestid22","auto.offset.reset"->"largest","auto.commit.enable"->"false") //largest
    var kafkaStream:InputDStream[(String,String)]=null
    var offsetRanges=Array[OffsetRange]()
     val sparkConf = new SparkConf().setAppName("Streaming Hbase")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.task.maxFailures","1")
    sparkConf.set("spark.speculation","false")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition",args(0))
     sparkConf.set("spark.sql.warehouse.dir","file:///")
    sparkConf.set("spark.debug.maxToStringFields", "10000")
    sparkConf.set("spark.driver.allowMultipleContexts", "true");
    sparkConf.set("spark.streaming.blockInterval","100ms")
    sparkConf.set("spark.default.parallelism","300")  
    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    val sc=new SparkContext(sparkConf)
    val cf="a"
    
     val ssc = new StreamingContext(sc, Seconds(args(1).toLong))
    val topic="firm_logs"
    val puts=new ArrayList();
    val topicDirs=new ZKGroupTopicDirs("spark_streaming_testid22", topic)
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
      }
     
    
    
   // kafkaStream.hbaseBulkPut(hc, tableName, f)
     val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181");
	    conf.set("hbase.rpc.timeout", "1800000");
	    conf.set("hbase.client.scanner.timeout.period", "1800000");
	    conf.set("mapreduce.task.timeout", "1800000");
   
   
    
/*	   kafkaStream.hbaseBulkPut(
      hbaseContext,
      TableName.valueOf("zrktest"),
      (putRecord) => {
        
        val pair=putRecord._2.split("@##@", -1)
        
        val put = new Put((pair(36).toLong%10+"#!"+pair(0)+"#"+pair(35) +"#"+ pair(36)).getBytes())
        
       put.addColumn("cf".getBytes(), "info".getBytes(), ("\""+pair(6)+"\" \""+pair(1)+"\" \""+pair(4)
			+"\" \""+pair(7)+"\" \""+pair(8)+"\" \""+pair(9)+"\" \""+pair(10)+"\" \""
			+pair(11)+"\" \""+pair(12)+"\" \""+pair(13)+"\" \""+pair(14)+
			"\" \""+pair(16)+"\"").getBytes())
			put.addColumn("cf".getBytes(), "info1".getBytes(),("\""+pair(0)+"\" \""+pair(2)+"\" \""+pair(3)
			+"\" \""+pair(5)+"\" \""+pair(15)+"\" \""+pair(17)+"\" \""+pair(18)+"\" \""
			+pair(19)+"\" \""+pair(20)+"\" \""+pair(21)+"\" \""+pair(22)+
			"\" \""+pair(23)+"\" \""+pair(24)+"\" \""+pair(25)+"\" \""+pair(26)+"\" \""
			+pair(27)+"\" \""+pair(28)+"\" \""+pair(29)+"\" \""+pair(30)+
			"\" \""+pair(31)+"\" \""+pair(32)+"\" \""+pair(33)+"\" \""+pair(34)+"\" \""+pair(35)+"\" \""+pair(36)+"\"").getBytes())
        put
      })*/
    
	    
	     val hbaseContext = new HBaseContext(sc, conf)
    val line=kafkaStream.map(x=>x._2)
    val chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    val rand=new Random(10)
    print("zrkprints+++++++++++++++++++++++"+line.count())
	   hbaseContext.streamBulkPut[String](line,
	        TableName.valueOf("zrktest"),
      (putRecord) => {
        
        val pair=putRecord.split("@##@", -1)
        print("zrkprints:pair0++++++++++"+pair(0))
        val put = new Put((chars.charAt(rand.nextInt(52))+"#!"+pair(0)+"#"+pair(35) +"#"+ pair(36)).getBytes())
        
       put.addColumn("cf".getBytes(), "info".getBytes(), ("\""+pair(6)+"\" \""+pair(1)+"\" \""+pair(4)
			+"\" \""+pair(7)+"\" \""+pair(8)+"\" \""+pair(9)+"\" \""+pair(10)+"\" \""
			+pair(11)+"\" \""+pair(12)+"\" \""+pair(13)+"\" \""+pair(14)+
			"\" \""+pair(16)+"\"").getBytes())
			put.addColumn("cf".getBytes(), "info1".getBytes(),("\""+pair(0)+"\" \""+pair(2)+"\" \""+pair(3)
			+"\" \""+pair(5)+"\" \""+pair(15)+"\" \""+pair(17)+"\" \""+pair(18)+"\" \""
			+pair(19)+"\" \""+pair(20)+"\" \""+pair(21)+"\" \""+pair(22)+
			"\" \""+pair(23)+"\" \""+pair(24)+"\" \""+pair(25)+"\" \""+pair(26)+"\" \""
			+pair(27)+"\" \""+pair(28)+"\" \""+pair(29)+"\" \""+pair(30)+
			"\" \""+pair(31)+"\" \""+pair(32)+"\" \""+pair(33)+"\" \""+pair(34)+"\" \""+pair(35)+"\" \""+pair(36)+"\"").getBytes())
        put
      })
	    
      kafkaStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //寰楀埌璇� rdd 瀵瑰簲 kafka 鐨勬秷鎭殑 offset
      rdd
    }.foreachRDD(rdd=>{
      
 
      print("zrkcount"+rdd.count())
      //rdd.foreach(s=>println(s))
     // offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //灏嗚 partition 鐨� offset 淇濆瓨鍒� zookeeper
      }
    })
    
    ssc.start()
    ssc.awaitTermination()

  }

  
  /*
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    val conn1 = DriverManager.getConnection(conn_str,user,password)

    try {
      val statement = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery("select * from achi limit 10")
      while (rs.next) {
        println(rs.getString(1))
      }
    }
    catch {
      case _ : Exception => println("===>")
    }
    finally {
      conn1.close
    }

    */
    
    
    
    



}