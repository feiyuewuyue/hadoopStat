package com.sinobbd.strider.dota.userlog
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import scala.util.Random
import scala.collection.mutable.HashMap
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql.EsSparkSQL
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import javax.ws.rs.GET
import org.apache.hadoop.io.NullWritable
import net.sf.json.JSONObject
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import java.util.Locale
import java.text.SimpleDateFormat
import org.apache.hadoop.io.compress.GzipCodec
import scala.collection.immutable.Map

object GenPkgFromES {
  def main(args: Array[String]) {
       // System.setProperty("hadoop.home.dir", "D:\\hadoop");
    val conf = new SparkConf().setAppName("GenPkgFromES")
        //  .setMaster("local[4]")
    conf.set("es.nodes", "183.136.128.47")
      .set("es.port", "9200").set("es.scroll.size", "2000").set("es.mapping.date.rich", "false")

    val sc = new SparkContext(conf)

    val params = getParam()
    for (prm <- params) {
      
      var startTimeWhole: Long = prm._1
      var endTimeWhole: Long = prm._2
       val qryIndex2 =prm._3// "nginx-20161211-20161211,nginx-20161212-20161211,nginx-lc-20161211-20161211,nginx-lc-20161212-20161211,nginx-st-20161211"
      //18:00--24点 一次跑1分钟数据，其余时间5分钟数据
      var time5m_sec = 300
      var startTime = startTimeWhole
      var endTime = startTimeWhole + time5m_sec
      while (endTime <= endTimeWhole) {
        // export/logs/downloadlogs_gz/20161101/00/分钟/bsy.imgoss.starschina.com
        //   val form:DateTimeFormatter =DateTimeFormat.forPattern("yyyyMMddHH").withLocale(Locale.US);

        //查询合作方为abc的数据
        val fields = """ ,"_source": ["clientip","response_time","time_local","time_unix","method","domain","request","protocol","http_status","body_bytes_sent","referer","user_agent","hit_status"] }"""

        //      val qryIndex = "nginx-20161213-20161213,nginx-20161214-20161213"
        //      val queryCond = """  {"query": {"bool": {"must": [{"terms":{"firm_name":["VC","UP"]}},{"range": {"time_unix": {"gte": """" + startTime + """","lt": """" + endTime + """"}}}]}}  """ + fields

       
        val queryCond2 = """{"query":{"bool":{"must":[{"range":{"time_unix":{"gte":"""" + startTime + """","lt":"""" + endTime + """"}}}]}} """

        //将在es中的查询结果转化为rdd/dataFrame
        //      val esRdd = sc.esRDD(qryIndex + s"/fluentd", queryCond + fields)
        val esRdd2 = sc.esRDD(qryIndex2 + s"/fluentd", queryCond2 + fields)
        //      val esRdd = sc.esJsonRDD(qryIndex + s"/fluentd", queryCond + fields)
        //      val esRdd2 = sc.esJsonRDD(qryIndex2 + s"/fluentd", queryCond2 + fields)

        val esRddAll = esRdd2.values //esRdd.union(esRdd2).values
        //    val esRddAllSorted=esRddAll.sortBy(f=>{f.get("domain").toString()+f.get("time_unix")}, true)
        //    val someRDD = spark.sparkContext.parallelize(List(("w", "www","sinobbd"), ("b", "blog","demo"), ("c", "com","sinabbd"), ("w", "bt","test")))
        //

        val targetRDD = esRddAll.mapPartitionsWithIndex((p, elms) => elms.map(f => genTxtFromValue(f)))

        val formatter = new SimpleDateFormat("yyyyMMdd-HH-mm");
        val dateString = formatter.format(startTime * 1000);
        val sdata = dateString.split("-")
        val path = "/export/logs/downloadlogs_gz/" + sdata.apply(0) + "/" + sdata.apply(1) + "/" + sdata.apply(2)
        val saveRdd = targetRDD.repartition(8) 
//        saveRdd.cache()
//      val count= saveRdd.map(f=>1)
        
       // println("--------total number--------TimeRange:["+startTime+"->"+endTime+"] Total Count:"+count.count()+"------------------")
        saveRdd.saveAsHadoopFile(path, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat], classOf[GzipCodec])
//        saveRdd.unpersist(false)
        val shour = sdata.apply(1)
        if (shour.compareTo("18") >= 0) {
          time5m_sec = 60
        }
        //跟新起止时间，进行下一次查询
        startTime = endTime
        endTime = startTime + time5m_sec

      }
    }
    sc.stop()
  }

  def getParam(): Array[Tuple3[Long, Long, String]] = {

   // val parm14 = Tuple3("1481644800".toLong, "1481731200".toLong, "nginx-20161214-20161214,nginx-20161215-20161214,nginx-lc-20161213-20161214,nginx-lc-20161214-20161214,nginx-lc-20161215-20161214,nginx-st-20161214")
   // val parm15 = Tuple3("1481731200".toLong, "1481817600".toLong, "nginx-20161215-20161215,nginx-20161216-20161215,nginx-20161217-20161215,nginx-lc-20161214-20161215,nginx-lc-20161215-20161215,nginx-lc-20161216-20161215,nginx-st-20161215")
//    val parm16 = Tuple3("1481817600".toLong, "1481904000".toLong, "nginx-20161215-20161216,nginx-20161216-20161216,nginx-20161217-20161216,nginx-20161218-20161216,nginx-lc-20161215-20161216,nginx-lc-20161216-20161216,nginx-lc-20161217-20161216,nginx-st-20161216")
//    val parm17 = Tuple3("1481904000".toLong, "1481990400".toLong, "nginx-20161216-20161217,nginx-20161217-20161217,nginx-20161218-20161217,nginx-lc-20161217-20161217,nginx-lc-20161218-20161217,nginx-st-20161217")
//    val parm18 = Tuple3("1481990400".toLong, "1482076800".toLong, "nginx-20161218-20161218,nginx-20161219-20161218,nginx-lc-20161218-20161218,nginx-lc-20161219-20161218,nginx-st-20161218")
//    val parm19 = Tuple3("1482076800".toLong, "1482163200".toLong, "nginx-20161219-20161219,nginx-20161220-20161219,nginx-lc-20161219-20161219,nginx-lc-20161220-20161219,nginx-st-20161219")
//    val parm20 = Tuple3("1482166800".toLong, "1482167100".toLong, "nginx-20161219-20161220,nginx-20161220-20161220,nginx-20161221-20161220,nginx-lc-20161220-20161220,nginx-lc-20161221-20161220,nginx-st-20161220")
   // val parm21 = Tuple3("1482249600".toLong, "1482336000".toLong, "nginx-20161220-20161221,nginx-20161221-20161221,nginx-20161222-20161221,nginx-lc-20161220-20161221,nginx-lc-20161221-20161221,nginx-lc-20161222-20161221,nginx-st-20161221")
   // val parm22 = Tuple3("1482336000".toLong, "1482422400".toLong, "nginx-20161221-20161222,nginx-20161222-20161222,nginx-20161223-20161222,nginx-lc-20161221-20161222,nginx-lc-20161222-20161222,nginx-lc-20161223-20161222,nginx-st-20161222")
   // val parm23 = Tuple3("1482422400".toLong, "1482508800".toLong, "nginx-20161222-20161223,nginx-20161223-20161223,nginx-20161224-20161223,nginx-lc-20161222-20161223,nginx-lc-20161223-20161223,nginx-lc-20161224-20161223,nginx-st-20161223")
    //val parm24 = Tuple3("1482508800".toLong, "1482595200".toLong, "nginx-20161223-20161224,nginx-20161224-20161224,nginx-20161225-20161224,nginx-lc-20161223-20161224,nginx-lc-20161224-20161224,nginx-lc-20161225-20161224,nginx-st-20161224")
    //val parm25 = Tuple3("1482595200".toLong, "1482681600".toLong, "nginx-20161224-20161225,nginx-20161225-20161225,nginx-20161226-20161225,nginx-lc-20161224-20161225,nginx-lc-20161225-20161225,nginx-lc-20161226-20161225,nginx-st-20161225")
    //val parm26 = Tuple3("1482681600".toLong, "1482768000".toLong, "nginx-20161225-20161226,nginx-20161226-20161226,nginx-20161227-20161226,nginx-lc-20161225-20161226,nginx-lc-20161226-20161226,nginx-lc-20161227-20161226,nginx-st-20161226")
    //val parm27 = Tuple3("1482843600".toLong, "1482854400".toLong, "nginx-20161227-20161227,nginx-lc-20161226-20161227,nginx-lc-20161227-20161227,nginx-st-20161227,nginx-st-20161228")
   // val parm28 = Tuple3("1482854400".toLong, "1482940800".toLong, "nginx-*-20161228,nginx-*-20161229")
   // val parm29 = Tuple3("1482940800".toLong, "1483027200".toLong, "nginx-*-20161229,nginx-*-20161230")
   // val parm30 = Tuple3("1483027200".toLong, "1483113600".toLong, "nginx-*-20161230,nginx-*-20161231")
//    val parm31 = Tuple3("1483196400".toLong, "1483200000".toLong, "nginx-*-20161231,nginx-*-20170101")
   // val parm01 = Tuple3("1483200000".toLong, "1483286400".toLong, "nginx-*-20170101")
   // val parm02 = Tuple3("1483286400".toLong, "1483372800".toLong, "nginx-*-20170102")
   // val parm03 = Tuple3("1483444800".toLong, "1483459200".toLong, "nginx-*-20170103")
    //val parm04 = Tuple3("1483527600".toLong, "1483545600".toLong, "nginx-*-20170104")
  //  val parm05 = Tuple3("1483628400".toLong, "1483632000".toLong, "nginx-*-20170105")
  //  val parm06 = Tuple3("1483632000".toLong, "1483718400".toLong, "nginx-*-20170106")
  //  val parm07 = Tuple3("1483718400".toLong, "1483720200".toLong, "nginx-*-20170107")
 //   val parm08 = Tuple3("1483804800".toLong, "1483891200".toLong, "nginx-*-20170108")
  //  val parm09 = Tuple3("1483891200".toLong, "1483977600".toLong, "nginx-*-20170109")
//    val parm10 = Tuple3("1484010900".toLong, "1484064000".toLong, "nginx-*-20170110")
  //  val parm11 = Tuple3("1484143200".toLong, "1484150400".toLong, "nginx-*-20170111")
//    val parm12 = Tuple3("1484236200".toLong, "1484236800".toLong, "nginx-*-20170112")
   // val parm13 = Tuple3("1484312400".toLong, "1484323200".toLong, "nginx-*-20170113")
//    val parm14 = Tuple3("1484388000".toLong, "1484409600".toLong, "nginx-*-20170114")
//    val parm15 = Tuple3("1484492400".toLong, "1484496000".toLong, "nginx-*-20170115")
//    val parm16 = Tuple3("1484560800".toLong, "1484582400".toLong, "nginx-*-20170116")
//    val parm17 = Tuple3("1484647200".toLong, "1484668800".toLong, "nginx-*-20170117")
//    val parm18 = Tuple3("1484733600".toLong, "1484755200".toLong, "nginx-*-20170118")
//    val parm19 = Tuple3("1484820000".toLong, "1484841600".toLong, "nginx-*-20170119")
//    val parm20 = Tuple3("1484906400".toLong, "1484928000".toLong, "nginx-*-20170120")
//    val parm21 = Tuple3("1484938800".toLong, "1485014400".toLong, "nginx-*-20170121")
//    val parm22 = Tuple3("1485014400".toLong, "1485100800".toLong, "nginx-*-20170122")
    val parm23 = Tuple3("1485154800".toLong, "1485187200".toLong, "nginx-*-20170123")
   
 // Array(parm13)
    Array(parm23)
  }

  def getString(mp: scala.collection.Map[String, AnyRef], key: String): String = {
    val value = mp.get(key).getOrElse("-")
    if (value == null||value.equals("/")) {
     return   "\"" + "-" + "\""
    } else {
     val valuestr=mp.get(key).getOrElse("-").toString()
       val valarray= valuestr.split("/")     
       if(valarray.length<=0){
         return "\"" + "-" + "\""
       }else   if(valarray.apply(0).length()>40){
        val domain=valarray.apply(0).substring(0,30)
          val valre="\"" + domain + "\""
           return valre;
      }else{
         val valre="\"" + valarray.apply(0) + "\""
         return valre;
      }
    
    
    }

  }
  val SEP = " "
  def getOutPutString(mp: scala.collection.Map[String, AnyRef]): String = {
    //  override def toString: String = s""""$clientip" "$ss" "$time_local" "$time_unix" "$method" "$domain" "$request" "$protocol" "$status" "$size" 
    //"$referer" "$agent" "$hit_status""""
    try {
      getString(mp, "clientip") + SEP + getString(mp, "response_time") + SEP + getString(mp, "time_local") + SEP + getString(mp, "method") + SEP + getString(mp, "domain") + SEP + getString(mp, "request") + SEP + getString(mp, "protocol") + SEP +
        getString(mp, "http_status") + SEP + getString(mp, "body_bytes_sent") + SEP + getString(mp, "referer") + SEP + getString(mp, "user_agent") + SEP + getString(mp, "hit_status")
    } catch {
      case ex: NullPointerException => {
        throw new RuntimeException(mp.toString(), ex)
      }
    }

  }

  def genTxtFromValue(qryValue: scala.collection.Map[String, AnyRef]): Tuple2[String, String] = {
    //    try {
    //      val map = new scala.collection.mutable.HashMap[String, AnyRef];
    //      //最外层解析
    //      val json = JSONObject.fromObject(jsonStr);
    //      val iterator = json.keys()
    //      while (iterator.hasNext()) {
    //        val key = iterator.next()
    //        val value = json.get(key);
    //        //如果内层还是数组的话，继续解析
    //        map.put(key.asInstanceOf[String], value);
    //      }
    val value = getOutPutString(qryValue)
    var key = qryValue.get("domain").getOrElse("-").toString() //getString(map, "domain")
    val end=key.indexOf("/")
    if(end>0){
       key=key.substring(0, end)
    }
  
    (key, value)
    //    } catch {
    //      case ex: Exception => {
    //        System.out.println("--------------转换json异常----------------------------" + qryValue)
    //        ex.printStackTrace()
    //        throw ex
    //      }
    //    }
  }
}

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val skey = key.asInstanceOf[String]
    if (skey.indexOf(":") > 0) {
      val array = skey.split(":")
      array.apply(0) + "-" + name
    } else {
      skey + "-" + name
    }
  }
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

}