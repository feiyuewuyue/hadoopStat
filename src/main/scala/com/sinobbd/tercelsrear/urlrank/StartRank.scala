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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.lang.Boolean
import com.sinobbd.hdfs.HDFSFile
import scala.collection.mutable.Map
@deprecated
object StartRank {
  def main(args: Array[String]) {
    //    System.setProperty("hadoop.home.dir", "D:\\hadoop");
    val topN = 2000
    //一次分析时间间隔
    val INTEAL = 3600

    val conf = new SparkConf().setAppName("sparkURLRank")
    //  .setMaster("local[4]")
    conf.set("es.index.auto.create", "true")
      .set("es.nodes", "183.136.128.47") //183.136.128.47:9000
      .set("es.port", "9200")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val SEP = "@##@"
    val SEP_Pre = "_"

    //example:   key1:value1/key2:value2
//    HDFSFile.createNewHDFSFile("hdfs://nameservice1/task/urlrank/config/stream_1h.properties", "beginTime:1485014400;endTime:1485018000" )

    val logTimeRange = getLogTimeRange()
    var timeBatch_from = logTimeRange._1
    var timeBatch_to = logTimeRange._1 + INTEAL;
    val endTime=logTimeRange._2
    while (timeBatch_to <= endTime) {
      val datetimeFrom = new DateTime(timeBatch_from * 1000)
      val strLogTime = datetimeFrom.toString("yyyyMMddHH")
      val storageDays = getStorageDays(datetimeFrom)
      val tmpPaths = getLogPath(storageDays, strLogTime)
      val paths = getExistPaths(tmpPaths)     
      if(paths.length<=0){
        println("-------------no exists files-------" + tmpPaths)
        return
      }
      val beanAllRdd = sc.textFile(paths.apply(0),8)
      val len = paths.length
      if (len > 1) {
        for (i <- 1 to len - 1) {
          val beanRddtmp = sc.textFile(paths.apply(i),8)
          beanAllRdd.union(beanRddtmp)
        }
      }
   
      val rddCombined = beanAllRdd.map(line => {
        val data = splitToData(line, SEP)
//        val random = new Random()
//        val prefix = random.nextInt(10);       

//        val rankkeys = prefix + SEP_Pre + data.id
         val rankkeys = data.id
        (rankkeys, data)
      })

      // System.out.println(beanRdd.collect().apply(1)._1 + "key" + beanRdd.collect().apply(1)._2.domain))
//      val rddReduceCombine = beanRdd.reduceByKey(sumFunc(_, _))
//      val rddCombined = rddReduceCombine.map(line => {
//        val originalkey = line._1.split(SEP_Pre)(1)
//        (originalkey, line._2)
//      })
      val rddreduced = rddCombined.reduceByKey(sumFunc(_, _)).map(line => {
        (line._2.domain, line._2)
      })
      rddreduced.cache()

      val begin = System.currentTimeMillis();
      val rddReq = rddreduced.topByKey(topN)(Ordering.by { x => {x.req_count.toString()+x.id }})
      val rddReqes = rddReq.flatMap(f => f._2)

      val indexName = "urlstats" + "-"
      rddReqes.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id"))//reqcount

      val rddTraffic = rddreduced.topByKey(topN)(Ordering.by { {x => x.traffic.toString()+x.id }})
      val rddTrafficEs = rddTraffic.flatMap(f => f._2)
      rddTrafficEs.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id"))//traffic
      //    //
      val rddDownloadNum = rddreduced.topByKey(topN)(Ordering.by { x => x.downloadnum.toString()+x.id })
      val rddDownloadNumES = rddDownloadNum.flatMap(f => f._2)
      rddDownloadNumES.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id"))//downloadnum
      //
      val rddreq_error = rddreduced.topByKey(topN)(Ordering.by { x => x.req_error.toString()+x.id })
      val rddreq_errorES = rddreq_error.flatMap(f => f._2)
      rddreq_errorES.saveToEs(indexName + "{time_local:YYYY.MM}" + "/urlrank", Map("es.mapping.id" -> "id"))//reqerror     
       timeBatch_from = timeBatch_to
      timeBatch_to = timeBatch_from + INTEAL;
      HDFSFile.createNewHDFSFile("hdfs://nameservice1/task/urlrank/config/stream_1h.properties", "beginTime:" + timeBatch_from.toString() + ";" + "endTime:" + timeBatch_to.toString())
     
    }

       sc.stop()
  }

  def getExistPaths(paths: List[String]): List[String] = {
    val conf = new Configuration();
    val fs = FileSystem.get(conf);
    var lst: List[String] = Nil
    paths.foreach { x =>
      {
        val statu = fs.globStatus(new Path(x))
        if (!statu.isEmpty) {
          lst = x :: lst
        }
      }
    }

    lst

  }

  def getLogPath(storageDays: Tuple3[String, String, String], strLogTime: String): List[String] = {

    //目录结构：flume/events/入库日期/入库小时/日志文件日期/日志时间-小时/域名/厂商/文件
    //    val path="hdfs://nameservice1/flume/events/20170116/10/20170116/2017011610/jxy.pp.starschinalive.com/LeCloud/*"

    // val path="hdfs://nn1:8020/flume/events/"+currTime+"/*/*/*/*/*"
    val path1 = "hdfs://nameservice1/flume/events_merge/" + storageDays._1 + "/*" + "/*/" + strLogTime + "/*/*/*"
    val path2 = "hdfs://nameservice1/flume/events_merge/" + storageDays._2 + "/*" + "/*/" + strLogTime + "/*/*/*"
    val path3 = "hdfs://nameservice1/flume/events_merge/" + storageDays._3 + "/*" + "/*/" + strLogTime + "/*/*/*"

    val paths = List(path1, path2, path3)
    paths
  }

  def getLogTimeRange(): Tuple2[Long, Long] = {
  
    val parms = HDFSFile.readHDFSFile("hdfs://nameservice1/task/urlrank/config/stream_1h.properties")
    println("-------------------file value:" + parms)
    val parmValue = parms.split(";")
    var mpValue: Map[String, String] = Map()
    parmValue.foreach { x =>
      {
        val kv = x.split(":")
        mpValue += (kv.apply(0) -> kv.apply(1))
      }
    }
    val default = (System.currentTimeMillis() / 1000 / 3600) * 3600 - 24 * 3600
    val timebegin = (mpValue.get("beginTime").getOrElse(default.toString())).toLong
    val timeEnd = (mpValue.get("endTime").getOrElse((timebegin + 3600).toString())).toLong
    val range = new Tuple2(timebegin, timeEnd)
    range
  
   // ("1485014400".toLong,"1485018000".toLong)
  }

  def getStorageDays(timeBatch_from: DateTime): Tuple3[String, String, String] = {
    //日志时间开始算起的三天，比如日志时间为16号，则考虑到数据延迟，需要扫描16,17,18号三天数据
    val currDay = timeBatch_from.toString("yyyyMMdd")
    val nextday = timeBatch_from.plusDays(1).toString("yyyyMMdd")
    val thirdDay = timeBatch_from.plusDays(2).toString("yyyyMMdd")
    val range = new Tuple3(currDay, nextday, thirdDay)
    range
  }

  def parseDouble(s: String): Double = { try { Some(s.toDouble).getOrElse(0.toDouble) } catch { case _: Throwable => 0.toDouble } }

  def parseAnyDouble(s: Option[Any]): Double = {
    s.getOrElse(0.toDouble).asInstanceOf[Double]
  }

  def getMd5(str: String): String = {

    val md5Key = DigestUtils.md5Hex(str);

    md5Key;

  }

  def splitToData(line: String, sep: String): urldata = {
    try{
    val linem = new HashMap[String, Any]
    val arrays = line.split(sep)
    //总数约34列
    if(arrays.length<30){     
       println("---error data---------"+line)
     return  urldata("-",new DateISO(System.currentTimeMillis()), System.currentTimeMillis(), "-", "-", 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0)
    }
    val stime_unix = arrays.apply(5)
    val time_unix = stime_unix.toLong
    //按小时取整
    val time_unix_fix = (time_unix / 3600) * 3600 * 1000
    val time_local = new DateISO(time_unix * 1000)
    //    val time_local=//tm.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val domain = arrays(8)
    val request = arrays(9)
    val traffic = parseDouble(arrays(12))
    val req_count = 1.toDouble
    val downloadnum = 0.toDouble
    val statusCode = arrays(11)
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
    }catch {
      case ex:Exception =>{
        throw new RuntimeException(ex.getMessage+"--data:"+line,ex)
      }
    }
  }
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
    println("req_error" + req_error)
    urldata(pre.id, pre.time_local, pre.time_unix_fix, domain, request, traffic, req_count, downloadnum, req_error, req401, req403, req404, req500, req501,
      req502, req503, req504)
  }
  case class urldata(id: String, time_local: DateISO, time_unix_fix: Long, domain: String, request: String, traffic: Double, req_count: Double,
                     downloadnum: Double, val req_error: Double,
                     req401: Double, req403: Double, req404: Double, req500: Double, req501: Double,
                     req502: Double, req503: Double, req504: Double)

}