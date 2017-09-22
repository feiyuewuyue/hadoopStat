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
@deprecated
object StartRankSql {
  def main(args: Array[String]) {
   // System.setProperty("hadoop.home.dir", "D:\\hadoop");
    val conf = new SparkConf().setAppName("sparkURLRank")
    //.setMaster("local[4]")
    conf.set("es.index.auto.create", "true")
      .set("es.nodes", "183.131.54.181")
      .set("es.port", "9200")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val SEP = " "
    val SEP_Pre = "_"
    val beanRdd = sc.textFile("/bigdata/test/urltop.log").map(line => {//D:\\bb\\urltop.log
      val data = splitToData(line, SEP)
      val random = new Random()
      val prefix = random.nextInt(10);
      val domain = data.domain
      val request = data.request
      val rankkeys = prefix + SEP_Pre + data.domain + SEP + data.request
      (rankkeys, data)
    })
    // System.out.println(beanRdd.collect().apply(1)._1 + "key" + beanRdd.collect().apply(1)._2.domain))
    val rddReduceCombine = beanRdd.reduceByKey(sumFunc(_, _))
    val rddCombined = rddReduceCombine.map(line => {
      val originalkey = line._1.split(SEP_Pre)(1)
      (originalkey, line._2)
    })
    val rddreduced = rddCombined.reduceByKey(sumFunc(_, _)).map(line => { line._2 })
    rddreduced.cache()
    val begin=System.currentTimeMillis();
    import sqlContext.implicits._
    val rdddf = rddreduced.toDF
    rdddf.registerTempTable("rankdata")
  val dfres=   sqlContext.sql("select * from  (select domain, request,traffic, req_count, downloadnum, req_error, req401, req403, req404, req500, req501, " +
      " req502, req503, req504,row_number() over (partition by domain order by traffic desc) as rowid from rankdata) rankres where rowid<=2")
      
      
      dfres.show()
      val end=System.currentTimeMillis()
      val exec=end-begin
      println("exec time:"+exec)

    //    val rddReq = rddreduced.topByKey(1)(Ordering.by { x => x.req_count})
    //    val rddReqes = rddReq.flatMap(f => f._2)
    //    rddReqes.collect().foreach { print }
    //    rddReqes.saveToEs("urlrank/reqcount")

    //    val rddTraffic = rddreduced.topByKey(1)(Ordering.by{ x => x.traffic})
    //    val rddTrafficEs = rddTraffic.flatMap(f => f._2)
    //    rddTrafficEs.saveToEs("urlrank/traffic")
    //
    //    val rddDownloadNum = rddreduced.topByKey(1)(Ordering.by { x => x.downloadnum})
    //    val rddDownloadNumES = rddDownloadNum.flatMap(f => f._2)
    //    rddDownloadNumES.saveToEs("urlrank/downloadnum")
    //
    //    val rddreq_error = rddreduced.topByKey(1)(Ordering.by { x => x.req_error})
    //    val rddreq_errorES = rddreq_error.flatMap(f => f._2)
    //    rddreq_errorES.saveToEs("urlrank/reqerror")

    sc.stop()
  }
  def parseDouble(s: String): Double = { try { Some(s.toDouble).getOrElse(0.toDouble) } catch { case _ => 0.toDouble } }

  //  def parseString(s: Option[Any]): String = {
  //    val tmp = s.getOrElse("-").toString()
  //    //    println("parseString-before:" + s)
  //    //    println("parseString-after" + tmp)
  //    tmp
  //  }
  def parseAnyDouble(s: Option[Any]): Double = {
    s.getOrElse(0.toDouble).asInstanceOf[Double]
  }

  def splitToData(line: String, sep: String): urldata = {
    val linem = new HashMap[String, Any]
    val arrays = line.split(sep)
    val domain = arrays(0)
    val request = arrays(1)
    val traffic = parseDouble(arrays(2))
    val req_count = parseDouble(arrays(3))
    val downloadnum = parseDouble(arrays(4))
    val statusCount = parseDouble(arrays(5))
    val statusCode = arrays(6)
    var req401 = 0.toDouble
    var req403 = 0.toDouble
    var req404 = 0.toDouble
    var req500 = 0.toDouble
    var req501 = 0.toDouble
    var req502 = 0.toDouble
    var req503 = 0.toDouble
    var req504 = 0.toDouble
    if (statusCode.equals("401")) {
      req401 = statusCount
    } else if (statusCode.equals("403")) {
      req403 = statusCount
    } else if (statusCode.equals("404")) {
      req404 = statusCount
    } else if (statusCode.equals("500")) {
      req500 = statusCount
    } else if (statusCode.equals("501")) {
      req501 = statusCount
    } else if (statusCode.equals("502")) {
      req502 = statusCount
    } else if (statusCode.equals("503")) {
      req503 = statusCount
    } else if (statusCode.equals("504")) {
      req504 = statusCount
    }
    val req_error = req401 + req403 + req404 + req500 + req501 + req502 + req503 + req504
    urldata(domain, request, traffic, req_count, downloadnum, req_error, req401, req403, req404, req500, req501,
      req502, req503, req504)
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
    urldata(domain, request, traffic, req_count, downloadnum, req_error, req401, req403, req404, req500, req501,
      req502, req503, req504)
  }
  case class urldata(domain: String, request: String, traffic: Double, req_count: Double,
                     downloadnum: Double, val req_error: Double,
                     req401: Double, req403: Double, req404: Double, req500: Double, req501: Double,
                     req502: Double, req503: Double, req504: Double)
}