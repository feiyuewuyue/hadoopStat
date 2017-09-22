package org.apache.spark.streaming.kafka
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import com.sinobbd.kafka.process.StreamProcess

import kafka.serializer.StringDecoder
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat
import org.apache.spark.HashPartitioner
import com.sinobbd.kafka.process.FieldConst
import java.lang.Long
import kafka.serializer.DefaultDecoder
import scala.collection.mutable.Map
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark._
import scala.collection.mutable.HashSet
import org.apache.avro.MD5
import org.apache.commons.codec.digest.DigestUtils
import com.sinobbd.kafka.process.RankSettingReader
import scala.collection.mutable.ListBuffer
import com.sinobbd.kafka.process.StreamProcess.urldata


object RankFromStream {
  val CHECKPOINT_DIR: String = RankSettingReader.configration.get("checkpint.dir.rank").toString()
  val batch_interval = RankSettingReader.configration.get("spark.rank.batch.interval_sec").toString()
  val kafkahosts = RankSettingReader.configration.get("kafka.hosts.rank").toString()
  val kafkaTopics = RankSettingReader.configration.get("kafka.topic.rank").toString()
  val kafkaGroupids = RankSettingReader.configration.get("kafka.groupid.rank.statis").toString()
  val esNodes = RankSettingReader.configration.get("es.nodes.rank").toString()
  val esPort = RankSettingReader.configration.get("es.port.rank").toString()
  val topN = 100

  def main(args1: Array[String]) {
    // args={ "test1:9092,test2:9092",}
    // System.setProperty("hadoop.home.dir", "D:\\hadoop");
    val ssc = functionToCreateContext //StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext)

    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext(): StreamingContext = {
    val args = Array(kafkahosts, kafkaTopics, kafkaGroupids) //test-consumer-group  
    val Array(brokers, topics, groupId) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaLogAnalyze")
    // sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //从consumer offsets到leader latest offsets中间延迟了很多消息，在下一次启动的时候，首个batch要处理大量的消息，
    //会导致spark-submit设置的资源无法满足大量消息的处理而导致崩溃。因此在spark-submit启动的时候多加了一个配置:
    //--conf spark.streaming.kafka.maxRatePerPartition=10000。限制每秒钟从topic的每个partition最多消费的消息条数，
    //这样就把首个batch的大量的消息拆分到多个batch中去了，为了更快的消化掉delay的消息，可以调大计算资源和把这个参数调大。
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1500")

    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.batch.size.bytes", "100mb")
    sparkConf.set("es.batch.size.entries", "10000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set("es.nodes", esNodes) //183.136.128.47:9200
    sparkConf.set("es.port", esPort)
    //sparkConf.set("spark.task.maxFailures", "1")
    // sparkConf.set("spark.speculation", "false")
    //sparkConf.set("spark.streaming.backpressure.enabled", "true")   

    //    sparkConf.set("spark.broadcast.compress", "false");
    //    sparkConf.set("spark.shuffle.compress", "false");
    //    sparkConf.set("spark.shuffle.spill.compress", "false");

    val ssc = new StreamingContext(sparkConf, Seconds(Long.valueOf(batch_interval)))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "largest") //smallest,largest

    val km = new KafkaManager(kafkaParams)

    val message = km.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)
    var offsetRanges = Array[OffsetRange]()
    val lines = message.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(f => {
        val line = StreamProcess.parseRankKafkaBytes(f._2)
        val dataUrl = StreamProcess.parseLogToRankURLData(line)
        val rankkeys = dataUrl.id
        (rankkeys, dataUrl)
      })
    }

    //    val reduceValue = lines.mapPartitions(mapPartFunc)
    val reducesec = lines.reduceByKeyAndWindow(StreamProcess.sumFunc(_, _), Seconds.apply(60), Seconds.apply(60))
    //每个域名保留topN条，其余数据过滤掉值req_count为1并且req_error=0，downloadnum=0行

    //   val reducesec = reducefirst.reduceByKeyAndWindow(StreamProcess.sumFunc(_, _), Seconds.apply(600), Seconds.apply(600))
   val reTrans=reducesec.map(line => {(line._2.domain, line._2)})
    reTrans.foreachRDD(rddreduced => {
      if (!rddreduced.isEmpty()) {

        rddreduced.cache()
        val rddReq = rddreduced.topByKey(topN)(Ordering.by { x => { x.req_count.toString() + x.id } })
        val rddReqes = rddReq.flatMap(f => setAndModifyIDArray(f._2, offsetRanges))

        val indexName = "urlstats" + "-"
        rddReqes.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "id")) //reqcount

        val rddTraffic = rddreduced.topByKey(topN)(Ordering.by { { x => x.traffic.toString() + x.id } })
        val rddTrafficEs = rddTraffic.flatMap(f => setAndModifyIDArray(f._2, offsetRanges))
        rddTrafficEs.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "id")) //traffic
        //    //
        val rddDownloadNum = rddreduced.topByKey(topN)(Ordering.by { x => x.downloadnum.toString() + x.id })
        val rddDownloadNumES = rddDownloadNum.flatMap(f => setAndModifyIDArray(f._2, offsetRanges))
        rddDownloadNumES.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "id")) //downloadnum
        //
        val rddreq_error = rddreduced.topByKey(topN)(Ordering.by { x => x.req_error.toString() + x.id })
        val rddreq_errorES = rddreq_error.flatMap(f => setAndModifyIDArray(f._2, offsetRanges))

        //  val rddUnionAll = rddReqes.union(rddTrafficEs).union(rddDownloadNumES).union(rddreq_errorES)
        //  val indexName = "urlstats" + "-"
        //  val rddSave = rddUnionAll.map(f => { setAndModifyID(f, offsetRanges) })
        rddreq_errorES.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "id")) //reqerror   

      }

      //一次更新60000/分区  共120个分区   10个excuter    spark 每次取10000/分区    每次Input Size 1200000 records     23个batch（23*12=27600000）     更新的offset 1380000  (165600000)
      km.updateZKOffsets(offsetRanges)
    })

    ssc
  }

  def setAndModifyIDArray(arrayDatas: Array[urldata], offsetRanges: Array[OffsetRange]): Array[urldata] = {
    val unique = StreamProcess.genPrefix(offsetRanges);
    val datas = new ListBuffer[urldata]
    for (hm <- arrayDatas) {
      val objID = hm.id
      var dataNew = hm
      if (!objID.isEmpty) {
        val s: StringBuffer = new StringBuffer()
        s.append(unique)
        s.append("##")
        s.append(objID)
        val md5Key1 = DigestUtils.md5Hex(unique);
          val md5Key2 = DigestUtils.md5Hex(objID);
            val md5Key = md5Key1+"-"+md5Key2;
        dataNew = urldata(md5Key, hm.time_local, hm.time_unix, hm.domain, hm.request, hm.traffic, hm.req_count, hm.downloadnum, hm.req_error, hm.req401, hm.req403, hm.req404, hm.req500, hm.req501,
          hm.req502, hm.req503, hm.req504)
        datas.append(dataNew)
      }
    }
    val arrayRes = datas.toArray
    arrayRes
  }

  def setAndModifyID(hm: urldata, offsetRanges: Array[OffsetRange]): urldata = {
    val unique = StreamProcess.genPrefix(offsetRanges);
    val objID = hm.id
    var dataNew = hm
    if (!objID.isEmpty) {
      val s: StringBuffer = new StringBuffer()
      s.append(unique)
      s.append("##")
      s.append(objID)
      val md5Key = DigestUtils.md5Hex(s.toString());
      dataNew = urldata(md5Key, hm.time_local, hm.time_unix, hm.domain, hm.request, hm.traffic, hm.req_count, hm.downloadnum, hm.req_error, hm.req401, hm.req403, hm.req404, hm.req500, hm.req501,
        hm.req502, hm.req503, hm.req504)

    }
    dataNew

  }
  def mapPartFunc(datas: Iterator[(String, StreamProcess.urldata)]): Iterator[(String, StreamProcess.urldata)] = {
    val datasMap = Map[String, ListBuffer[(String, StreamProcess.urldata)]]()
    while (datas.hasNext) {
      val element = datas.next()
      val urldata = element._2
      val domain = urldata.domain
      val lstBuffer = datasMap.get(domain)
      if (lstBuffer.isEmpty) {
        val buffer = new ListBuffer[(String, StreamProcess.urldata)]
        buffer.append(element)
        datasMap.put(domain, buffer)
      } else {
        val listBuffer = lstBuffer.get
        if (listBuffer.size > topN) {
          if (urldata.req_count > 1 || urldata.req_error > 0) {
            listBuffer.append(element)
          }
        } else {
          listBuffer.append(element)
        }
      }

    }
    val allData = new ListBuffer[(String, StreamProcess.urldata)]
    val dataMapValue = datasMap.values
    dataMapValue.foreach(f => { allData.appendAll(f.iterator) })
    allData.iterator
  }

}