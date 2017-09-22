package com.zrk.mllibtest

import org.apache.kafka.common.TopicPartition
import kafka.utils.ZkUtils
import kafka.utils.ZKGroupTopicDirs
import scala.collection.mutable.ListBuffer

import scala.collection.JavaConversions
import org.apache.zookeeper.data.ACL
import kafka.message.MessageAndMetadata
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD

import org.apache.spark.streaming.dstream.InputDStream
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder
import com.metamx.common.logger.Logger
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.OffsetRange
import kafka.utils.ZkUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Time


object CustomDirectKafkaExample {
  
 
  val sparkConf = new SparkConf()
//  val logger = Logger.getLogger(CustomDirectKafkaExample.getClass)

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.exit(1)
    }
    sparkConf.setAppName("CustomDirectKafkaExample") //  setting spark conf parameters
  //  sparkStreamingConf.foreach { x => val split = x.split("="); sparkConf.set(split(0), split(1));}
    val sc = new SparkContext(sparkConf) //create spark and memsql context
    //  declare checkpoint directory,pass to getOrCreate() method
    val Array(brokers, topics) = args
 //   val checkpointDir = CHECKPOINT_DIRECTORY_REQUEST
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicsSet = topics.split(",").toSet
   // val ssc =  setupSsc(topicsSet, kafkaParams, msc)
    val ssc= new StreamingContext(sc, Seconds(args(2).toLong))
    val messages = createCustomDirectKafkaStream(ssc,kafkaParams,"localhost","/kafka", topicsSet)
    /* Start the spark streaming   */
    ssc.start()
    ssc.awaitTermination();
  }//main() ends

/*  def setupSsc(topicsSet: Set[String], kafkaParams: Map[String, String])(): StreamingContext = {
 //   val sc = msc.sparkContext
     val sc=new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(15))
     create direct kafka stream 
    val messages = createCustomDirectKafkaStream(ssc,kafkaParams,"localhost","/kafka", topicsSet)
    val line = messages.map(_._2)
    val lines = line.flatMap(line => line.split("\n"))
    val filterLines = lines.filter { x => x.contains("zzr") }
    filterLines.foreachRDD((rdd: RDD[String], time: Time) => {
      rdd.foreachPartition { partitionOfRecords =>
        {if(partitionOfRecords.isEmpty)
          {
           // logger.info("partitionOfRecords FOUND EMPTY ,IGNORING THIS PARTITION")}
          }
          else
          {
             write computation logic here  
        }//else loop ends
      } //partition ends
       }
    ssc}
  )}*/
    //setUp(ssc) ends
  /* createDirectStream() method overloaded */
  def createCustomDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], zkHosts: String
                                    , zkPath: String, topics: Set[String]): InputDStream[(String, String)] = {
    val topic = topics.last //TODO only for single kafka topic right now
    val zkClient = new ZkClient(zkHosts, 30000, 30000)
    val storedOffsets = readOffsets(zkClient,zkHosts, zkPath, topic)
    val kafkaStream = storedOffsets match {
      case None => // start from the latest offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) => // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder
          , (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
        }
    // save the offsets
    kafkaStream.foreachRDD(rdd => saveOffsets(zkClient,zkHosts, zkPath, rdd))
    kafkaStream
  }

  /*
   Read the previously saved offsets from Zookeeper
    */
  private def readOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, topic: String):
  Option[Map[TopicAndPartition, Long]] = {
   // logger.info("Reading offsets from Zookeeper")
    val stopwatch = new Stopwatch()
    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
    //    logger.info(s"Read offset ranges: ${offsetsRangesStr}")
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
          .toMap
  //      logger.info("Done reading offsets from Zookeeper. Took " + stopwatch)
        Some(offsets)
      case None =>
  //      logger.info("No offsets found in Zookeeper. Took " + stopwatch)
        None
    }
  }

  private def saveOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, rdd: RDD[_]): Unit = {
//    logger.info("Saving offsets to Zookeeper")
    val stopwatch = new Stopwatch()
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
   // offsetsRanges.foreach(offsetRange => logger.debug(s"Using ${offsetRange}"))
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
//    logger.info("chandan Writing offsets to Zookeeper zkClient="+zkClient+"  zkHosts="+zkHosts+"zkPath="+zkPath+"  offsetsRangesStr:"+ offsetsRangesStr)
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
 //   logger.info("Done updating offsets in Zookeeper. Took " + stopwatch)
  }

  class Stopwatch {
    private val start = System.currentTimeMillis()
    override def toString() = (System.currentTimeMillis() - start) + " ms"
  }


}