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
import java.util.UUID


object streamingToHbase extends Serializable{
  
  
  @transient lazy val log = LogManager.getRootLogger
  def functionToCreateContext(args: Array[String]): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("sparkToHbaseRH")
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

    //do something....
    val keyvalue=kafkaStream.map(data=>{
        (UUID.randomUUID().toString(),data._2)
    }).map { item =>
      val rowKey = item._1
      val value = item._2
      convertToHbasePut(rowKey, value, table)
	  }
       keyvalue.foreachRDD { rdd =>
      
      rdd.saveAsNewAPIHadoopFile("", classOf[ImmutableBytesWritable], classOf[Put], classOf[MultiTableOutputFormat], jobConf)
       
    }

    //更新zk中的offset
    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        km.updateZKOffsets(rdd)
    })

    ssc
  }

   def convertToHbasePut(key: String, value: String, tableName: String): (ImmutableBytesWritable, Put) = {
    val rowKey = key
    val ts=System.currentTimeMillis();
    val put = new Put(Bytes.toBytes(rowKey),ts)
    if(!value.equals("")){
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info"), Bytes.toBytes(value))
    }  
    (new ImmutableBytesWritable(Bytes.toBytes(tableName)), put)
  }

  def main(args: Array[String]) {
    val ssc = functionToCreateContext(args)
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  
}