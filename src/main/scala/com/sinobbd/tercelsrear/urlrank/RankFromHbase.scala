package com.sinobbd.tercelsrear.urlrank
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import scala.util.Random
import scala.collection.mutable.HashMap
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql.EsSparkSQL
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import java.util.Date
import org.joda.time.DateTime
import java.util.Formatter.DateTime
import org.apache.commons.codec.digest.DigestUtils
import java.lang.Boolean
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import com.sinobbd.strider.dota.DateFormatBBD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{ Base64, Bytes }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
//import java.util.Arrays.ArrayList
import java.util.ArrayList
import scala.collection.mutable.Map
import org.elasticsearch.spark._
import com.sinobbd.kafka.process.StreamProcess
//import org.apache.hadoop.fs.shell.find.Print
import com.sinobbd.hdfs.HDFSFile
import com.sinobbd.strider.dota.band.BusinessProcess
import com.sinobbd.data.merge.ParseSrcData
import scala.collection.JavaConverters
import com.sinobbd.kafka.process.FieldConst
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import com.sinobbd.kafka.process.StreamProcess.urldata

@deprecated
object RankFromHbase {
  val column_family = "cf"
  val column_log = "info"
  val column_other = "info1"
  val INTEAL = 600 * 1000
  val topN = 2000
  def main(args: Array[String]) {
    //  System.setProperty("hadoop.home.dir", "D:\\hadoop");

    //一次分析时间间隔

    val conf = new SparkConf().setAppName("sparkURLRank")
    // .setMaster("local[4]")
    conf.set("es.index.auto.create", "true")
      .set("es.nodes", "183.136.128.47") //183.136.128.47:9000
      .set("es.port", "9200")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取配置，获取要计算的起止时间
    val logTimeRange = getLogTimeRange()
    //计算的开始时间
    var timeBatch_from = logTimeRange._1
    //第一个批次要计算的截止时间
    var timeBatch_to = logTimeRange._1 + INTEAL;
    //计算的结束时间
    val endTime = logTimeRange._2

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "183.131.54.142,183.131.54.143,183.131.54.144")

    //    while (timeBatch_to <= endTime) {
    //      val datetimeFrom = new DateTime(timeBatch_from * 1000)

    val qryJXInfo = getJXTableInfoByLogTime(timeBatch_from, timeBatch_to);
    //读取JX数据
    //  val jxRdd = getJXRdd(sc, hbaseConf, qryJXInfo)

    val fusionArray = getFusionTableInfoByLogTime(timeBatch_from, timeBatch_to);
    //读取融合CDN数据
    val fusionRdd = getFusionRdd(sc, hbaseConf, fusionArray.apply(0))
    //      val fusionRddLag = getFusionRdd(sc, hbaseConf, fusionArray.apply(1))

    val rddCombinedtmp = fusionRdd //jxRdd//.union(fusionRdd).union(fusionRddLag)
    print("jxRdd分区个数====>" + fusionRdd.getNumPartitions)
    print("rddCombined分区个数====>" + rddCombinedtmp.getNumPartitions)
    val rddCombined = rddCombinedtmp //.coalesce(32, false)
    // System.out.println(beanRdd.collect().apply(1)._1 + "key" + beanRdd.collect().apply(1)._2.domain))
    //      val rddReduceCombine = beanRdd.reduceByKey(sumFunc(_, _))
    //      val rddCombined = rddReduceCombine.map(line => {
    //        val originalkey = line._1.split(SEP_Pre)(1)
    //        (originalkey, line._2)
    //      })
    val rddreduced = rddCombined.reduceByKey(sumFunc(_, _)).map(line => {
      (line._2.domain, line._2)
    })
   val rddReadys=  rddreduced.map( line => {(line._2.domain, line._2)})
   
    rddReadys.cache()

    val begin = System.currentTimeMillis();
    val rddReq = rddReadys.topByKey(topN)(Ordering.by { x => { x.req_count.toString() + x.id } })
    val rddReqes = rddReq.flatMap(f => f._2)

    val indexName = "urlstats" + "-"
    rddReqes.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id")) //reqcount

    val rddTraffic = rddReadys.topByKey(topN)(Ordering.by { { x => x.traffic.toString() + x.id } })
    val rddTrafficEs = rddTraffic.flatMap(f => f._2)
    rddTrafficEs.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id")) //traffic
    //    //
    val rddDownloadNum = rddReadys.topByKey(topN)(Ordering.by { x => x.downloadnum.toString() + x.id })
    val rddDownloadNumES = rddDownloadNum.flatMap(f => f._2)
    rddDownloadNumES.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id")) //downloadnum
    //
    val rddreq_error = rddReadys.topByKey(topN)(Ordering.by { x => x.req_error.toString() + x.id })
    val rddreq_errorES = rddreq_error.flatMap(f => f._2)
    rddreq_errorES.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id")) //reqerror     
    timeBatch_from = timeBatch_to
    timeBatch_to = timeBatch_from + INTEAL;
    HDFSFile.createNewHDFSFile("hdfs://nameservice1/task/urlrank/config/stream_1h.properties", "beginTime:" + timeBatch_from.toString() + ";" + "endTime:" + timeBatch_to.toString())

    //    }

    sc.stop()
  }

  def getJXRdd(sc: SparkContext, hbaseConf: Configuration, qryInfo: Tuple3[String, String, String]): RDD[(String, urldata)] = {
    val tableName = qryInfo._1
    val fromTime = qryInfo._2
    val toTime = qryInfo._3
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val scan = new Scan()
    val rowkeyPartValueBegin = "#!" + fromTime
    val rowkeyPartValueEnd = "#!" + toTime
    val ranges = new ArrayList[RowRange]()
    print(tableName -> (fromTime + "-" + toTime))
    for (a <- 0 to 9) {
      ranges.add(new MultiRowRangeFilter.RowRange((a.toString() + rowkeyPartValueBegin), true, (a.toString() + rowkeyPartValueEnd), false))

    }

    val filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(column_log));
    scan.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(column_other));
    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val usersRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
    val lines = usersRDD.map(f => {
      val result = f._2
      val userlog = Bytes.toString(result.getValue(column_family.getBytes, column_log.getBytes))
      val otherlog = Bytes.toString(result.getValue(column_family.getBytes, column_other.getBytes))
      val dataUrl = parseJXLogToURLData(userlog, otherlog)
      val rankkeys = dataUrl.id
      (rankkeys, dataUrl)

    })
    //    print( lines.take(3))
    val mergerdd = lines.mapPartitions(f => { mergeDetailLog(f) })
    mergerdd

  }
  def mergeDetailLog(valueIter: Iterator[Tuple2[String, urldata]]): Iterator[Tuple2[String, urldata]] = {
    val hmResult = Map[String, Tuple2[String, urldata]]()

    for (datas <- valueIter) {
      val dataRes = hmResult.get(datas._1);
      if (dataRes.isEmpty) {
        hmResult.put(datas._1, datas);
      } else {
        val predata = dataRes.get._2
        val secdata = datas._2
        val merged = sumFunc(predata, secdata)
        hmResult.put(datas._1, (datas._1, merged))
      }

    }
    hmResult.valuesIterator

  }

  def getFusionRdd(sc: SparkContext, hbaseConf: Configuration, qryInfo: Tuple3[String, String, String]): RDD[(String, urldata)] = {
    val tableName = qryInfo._1
    val fromTime = qryInfo._2
    val toTime = qryInfo._3
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val scan = new Scan()
    val rowkeyPartValueBegin = "#!" + fromTime
    val rowkeyPartValueEnd = "#!" + toTime
    val ranges = new ArrayList[RowRange]()
    for (a <- 0 to 9) {
      ranges.add(new MultiRowRangeFilter.RowRange((a.toString() + rowkeyPartValueBegin), true, (a.toString() + rowkeyPartValueEnd), false))

    }

    val filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(column_log));
    scan.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(column_other));
    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val usersRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
    val lines = usersRDD.map(f => {
      val result = f._2
      val userlog = Bytes.toString(result.getValue(column_family.getBytes, column_log.getBytes))
      val otherlog = Bytes.toString(result.getValue(column_family.getBytes, column_other.getBytes))
      val dataUrl = parseFusionLogToURLData(userlog, otherlog)
      val rankkeys = dataUrl.id
      (rankkeys, dataUrl)

    })
//    print("总数量====》" + lines.count())
    val mergerdd = lines.mapPartitions(f => {
      val merged = mergeDetailLog(f)
      val reduced = mapPartFunc(merged)
      reduced
    })

    mergerdd
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
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def getJXTableInfoByLogTime(startTime: Long, endTime: Long): Tuple3[String, String, String] = {
    val strBegin = DateFormatBBD.convertToStrYMDHM(startTime)
    val strEnd = DateFormatBBD.convertToStrYMDHM(endTime)
    val tablesubfix = DateFormatBBD.convertToStrYMDNODot(startTime)
    val tableName = "jx" + tablesubfix
    val range = new Tuple3(tableName, strBegin, strEnd)
    range
  }

  def getFusionTableInfoByLogTime(startTime: Long, endTime: Long): Array[Tuple3[String, String, String]] = {
    val strBegin = DateFormatBBD.convertToStrYMDHM(startTime)
    val strEnd = DateFormatBBD.convertToStrYMDHM(endTime)
    val tablesuffix = DateFormatBBD.convertToStrYMDNODot(startTime)
    val tableLagsuffix = DateFormatBBD.convertToStrYM_NoDot(startTime)
    val tableName = "nginx" + tablesuffix
    val tableNameLag = "nginx" + tableLagsuffix
    val range = new Tuple3(tableName, strBegin, strEnd)
    val range_lag = new Tuple3(tableName, strBegin, strEnd)
    Array(range, range_lag)
  }

  def getLogTimeRange(): Tuple2[Long, Long] = {

    //    val parms = HDFSFile.readHDFSFile("hdfs://nameservice1/task/urlrank/config/stream_1h.properties")
    //    println("-------------------file value:" + parms)
    //    val parmValue = parms.split(";")
    var mpValue: Map[String, String] = Map()
    //    parmValue.foreach { x =>
    //      {
    //        val kv = x.split(":")
    //        mpValue += (kv.apply(0) -> kv.apply(1))
    //      }
    //    }
    val default =1492344000000L// (System.currentTimeMillis() / 1000 / 3600) * 3600 * 1000 - 24 * 3600 * 1000
    val timebegin = (mpValue.get("beginTime").getOrElse(default.toString())).toLong
    val timeEnd = (mpValue.get("endTime").getOrElse((timebegin + INTEAL).toString())).toLong
    val range = new Tuple2(timebegin, timeEnd)
    range

    //     ("1491840000000".toLong,"1491843600000".toLong)
  }

  def parseDouble(s: String): Double = { try { Some(s.toDouble).getOrElse(0.toDouble) } catch { case _: Throwable => 0.toDouble } }

  def parseAnyDouble(s: Option[Any]): Double = {
    s.getOrElse(0.toDouble).asInstanceOf[Double]
  }

  def getMd5(str: String): String = {

    val md5Key = DigestUtils.md5Hex(str);

    md5Key;

  }
  def splitToData(line: Map[String, Any]): urldata = {
    try {
      val linem = new HashMap[String, Any]

      val stime_unix = line.get(FieldConst.field_time_unix).getOrElse("0").toString()
      val time_unix = stime_unix.toDouble.longValue()
      //按小时取整
      val time_unix_fix = (time_unix / 3600) * 3600 * 1000
      val time_local = new DateISO(time_unix * 1000)

      val domain = line.get(FieldConst.field_domain).getOrElse("-").toString()
      val request = line.get(FieldConst.field_request).getOrElse("-").toString()
      val traffic = parseDouble(line.get(FieldConst.field_size).getOrElse("0").toString())
      val req_count = 1.toDouble
      val downloadnum = 0.toDouble
      val statusCode = line.get(FieldConst.field_http_status).getOrElse("0")
      val ONE = 1.toDouble
      val ZERO = 0.toDouble
      var req401 = ZERO
      var req403 = ZERO
      var req404 = ZERO
      var req500 = ZERO
      var req501 = ZERO
      var req502 = ZERO
      var req503 = ZERO
      var req504 = ZERO
      var req_error = ZERO

      if (statusCode.equals("401")) {
        req401 = ONE
        req_error = ONE
      } else if (statusCode.equals("403")) {
        req403 = ONE
        req_error = ONE
      } else if (statusCode.equals("404")) {
        req404 = ONE
        req_error = ONE
      } else if (statusCode.equals("500")) {
        req500 = ONE
        req_error = ONE
      } else if (statusCode.equals("501")) {
        req501 = ONE
        req_error = ONE
      } else if (statusCode.equals("502")) {
        req502 = ONE
        req_error = ONE
      } else if (statusCode.equals("503")) {
        req503 = ONE
        req_error = ONE
      } else if (statusCode.equals("504")) {
        req504 = ONE
        req_error = ONE
      }
      //域名+req+时间作为key
      val key = domain + request + time_unix_fix
      val id = getMd5(key)

      urldata(id, time_local, time_unix_fix, domain, request, traffic, req_count, downloadnum, req_error, req401, req403, req404, req500, req501,
        req502, req503, req504)
    } catch {
      case ex: Exception => {
        throw new RuntimeException(ex.getMessage + "--data:" + line, ex)
      }
    }
  }
  def parseJXLogToURLData(userlog: String, otherlog: String): urldata = {
    val mpHbase = ParseSrcData.splitJXHbase2Map(userlog, otherlog);
    val mp = JavaConverters.mapAsScalaMapConverter(mpHbase).asScala
    val line = mp.asInstanceOf[Map[String, Any]]
    val res = splitToData(line)
    res
  }

  def parseFusionLogToURLData(userlog: String, otherlog: String): urldata = {
    val mpHbase = ParseSrcData.splitFusionHbase2Map(userlog, otherlog);
    val mp = JavaConverters.mapAsScalaMapConverter(mpHbase).asScala
    val line = mp.asInstanceOf[Map[String, Any]]
    val res = splitToData(line)
    res
  }

  //  case class urldata(id: String, time_local: DateISO, time_unix_fix: Long, domain: String, request: String, traffic: Double, req_count: Double,
  //                     downloadnum: Double, val req_error: Double,
  //                     req401: Double, req403: Double, req404: Double, req500: Double, req501: Double,
  //                     req502: Double, req503: Double, req504: Double)

  def sumFunc(pre: urldata, after: urldata): urldata = {
    val domain = pre.domain
    val request = pre.request
    val traffic = pre.traffic + after.traffic
    val req_count = pre.req_count + after.req_count
    val downloadnum = pre.downloadnum + after.downloadnum
    //    val req_error = parseAnyDouble(pre.req_error)) + parseAnyDouble(after.req_error))
    val req401 = pre.req401 + after.req401
    val req403 = pre.req403 + after.req403
    val req404 = pre.req404 + after.req404
    val req500 = pre.req500 + after.req500
    val req501 = pre.req501 + after.req501
    val req502 = pre.req502 + after.req502
    val req503 = pre.req503 + after.req503
    val req504 = pre.req504 + after.req504
    val req_error = req401 + req403 + req404 + req500 + req501 + req502 + req503 + req504
    //    println("req_error" + req_error)
    urldata(pre.id, pre.time_local, pre.time_unix, domain, request, traffic, req_count, downloadnum, req_error, req401, req403, req404, req500, req501,
      req502, req503, req504)
  }

}