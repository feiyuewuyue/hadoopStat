package com.sinobbd.strider.dota.band
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
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
import java.util.Arrays.ArrayList
import java.util.ArrayList
import scala.collection.mutable.Map
import org.elasticsearch.spark._
import com.sinobbd.kafka.process.StreamProcess
import com.sinobbd.kafka.process.BasicSettingReader
import org.apache.hadoop.fs.shell.find.Print
import scala.collection.mutable.ArrayBuffer
import com.sinobbd.strider.dota.DateFormatBBD
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.FuzzyRowFilter
import java.util.Arrays
import org.joda.time.DateTime
import com.sinobbd.kafka.process.BandCheckProcess

/**
 * 比较Hbase明细日志和ES中带宽计算结果，用来校对带宽数据
 * 维度：文件名、5分钟时间段
 */
object FusionBandCheck {
  val column_family = "cf"
  val column_log = "info"
  val column_other = "info1"
  val esIndex="statsdata_5m_"
  val hbaseTablePrefix="nginx"
   val INTEAL = 4 * 300 * 1000
  //  val esNodes = BasicSettingReader.configration.get("es.nodes.jx").toString()
  //  val esPort = BasicSettingReader.configration.get("es.port.jx").toString()

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
  //  def main(args: Array[String]) {
  //     val currDate = new DateTime().minusDays(3);
  //    val theNextDay = new DateTime().minusDays(2);
  //    val currDateBegin = currDate.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
  //    val currDateEnd = theNextDay.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
  //
  //      val timeBegin = currDateBegin.getMillis //DateFormatBBD.convertYMDHMtoMill(fromTimeWhole)
  //    val timeEnd = currDateEnd.getMillis //DateFormatBBD.convertYMDHMtoMill(toTimeWhole)
  //    val arrayStr = getLogTimeRange(timeBegin, timeEnd)
  //    
  //  
  //
  //  }

  def main(args: Array[String]) {
    if (!args.isEmpty) {
      for (key <- args) {
        print("====args====" + key)
      }
    }
    if (args == null || args.length < 1) {
      print("请输入参数：1：ES Client IP 2：核对日期（yyyyMMdd）")
      return
    }
    var esNodes = args.apply(0) //"183.131.54.181"
    var currDate = new DateTime().minusDays(3);
    if (args.length >= 2) {
      val dateBegin = args.apply(1)
      currDate = DateFormatBBD.convertNoDotToDateTime(dateBegin)
    }
    // val currDate = new DateTime().minusDays(3);
    val theNextDay = new DateTime().minusDays(2);
    val currDateBegin = currDate.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
    val currDateEnd = theNextDay.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);

    //    val fromTimeWhole = args.apply(0) // "2017040500"
    //    val toTimeWhole = args.apply(1) //"2017040501"

    var firmName: String = "all"
    //    if (args.length >= 4) {
    //      firmName = args.apply(3)
    //    }
    val timeBegin = currDateBegin.getMillis //DateFormatBBD.convertYMDHMtoMill(fromTimeWhole)
    val timeEnd = currDateEnd.getMillis //DateFormatBBD.convertYMDHMtoMill(toTimeWhole)
    val arrayStr = getLogTimeRange(timeBegin, timeEnd)

    val sparkConf = new SparkConf().setAppName("BandwidthOnHbase") //.setMaster("local")
    //  System.setProperty("hadoop.home.dir", "D:\\hadoop");
    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.batch.size.bytes", "100mb")
    sparkConf.set("es.batch.size.entries", "10000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set("es.nodes", esNodes) //183.136.128.47:9200
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.mapping.date.rich", "false")
    sparkConf.set("es.scroll.size", "2000")

    val sc = new SparkContext(sparkConf)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "183.131.54.142,183.131.54.143,183.131.54.144")

    for (param <- arrayStr) {
      val tableName = param._1 //"nginx20170602"
      val tableLagName = param._2 //"nginx201706"
      val fromTime = param._3 // "2017040500"
      val toTime = param._4 //"2017040501"
      val scan = new Scan()

      //根据时间过滤
      val rowkeyPartValueBegin = "#!" + fromTime
      val rowkeyPartValueEnd = "#!" + toTime
      val ranges = new ArrayList[RowRange]()
      for (a <- 0 to 9) {
        ranges.add(new MultiRowRangeFilter.RowRange((a.toString() + rowkeyPartValueBegin), true, (a.toString() + rowkeyPartValueEnd), false))

      }
      val filter = new MultiRowRangeFilter(ranges);
      val filterList = new FilterList(filter)

      //如果有厂商条件，根据厂商过滤 
      if (!firmName.equals("all")) {
        var rwkey = "0#!201705010000#!"
        // var maskBuffer= Array(1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte, 1.toByte)
        val mask = new ListBuffer[Byte]
        for (i <- 1 to rwkey.length()) {
          mask.append(1.toByte)
        }
        rwkey = rwkey + firmName + "#"
        for (i <- 1 to firmName.length() + 1) {
          mask.append(0.toByte)
        }
        val btyValue = Bytes.toBytesBinary(rwkey)
        val bytMask = mask.toArray

        val pair = new org.apache.hadoop.hbase.util.Pair(btyValue, bytMask)
        val listpari = List(pair)
        val fuzzyFilter = new FuzzyRowFilter(listpari)
        filterList.addFilter(fuzzyFilter)
      }

      scan.setFilter(filterList);

      scan.setCaching(500);
      scan.setCacheBlocks(false);
      //      scan.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(column_log));
      //      scan.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(column_other));
      hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))
      hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
      val usersRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
      hbaseConf.set(TableInputFormat.INPUT_TABLE, tableLagName)
      val usersLagRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

      val userLogRdd = usersRDD.union(usersLagRDD)
      val lines = userLogRdd.map(f => {
        val rowkeyBtye = f._1.get
        val rowkey = Bytes.toString(rowkeyBtye)
        val res = BandCheckProcess.transRowkeyToFileInfo(rowkey)
        res
      })
      //      val lines = userLogRdd.map(f => {
      //        val result = f._2
      //        val userlog = Bytes.toString(result.getValue(column_family.getBytes, column_log.getBytes))
      //        val otherlog = Bytes.toString(result.getValue(column_family.getBytes, column_other.getBytes))
      //        val hmValue = BusinessProcess.parseFusionLog(userlog, otherlog)
      //        val res = BandCheckProcess.transDetailLogToFileTraffic(hmValue)
      //        res
      //      })
      val rddRes = lines.reduceByKey((valueMap0, valueMap1) => StreamProcess.mergeDataStatis(valueMap0, valueMap1))

      val esIndex = param._5 //"statsdata_5m_2017.05.03"     
      val startTime = param._6 // "2017040500"
      val endTime = param._7 //"2017040501"
      val queryCond = """{"query":{"bool":{"must":[{"range":{"from_time":{"gte":"""" + startTime + """","lt":"""" + endTime + """"}}} ]}}} """
      
      //val queryCond = """{"query":{"bool":{"must":[{  "term": {"file_name": "8522390_wapscdn.wapx.cn_20170508002000_WS.access_48674" } },{"range":{"from_time":{"gte":"""" + startTime + """","lt":"""" + endTime + """"}}} ]}}} """
      val esRdd = sc.esRDD(esIndex + s"/bandwidth", queryCond)
      val esLines = esRdd.map(f => {
        val hmValue = f._2
        val res = BandCheckProcess.transBandToTuple5m(hmValue.asInstanceOf[Map[String, Any]])
        res
      })
      if (esLines.isEmpty()) {
        println("没有查到带宽" + queryCond)
      } else {
        // esLines.collect().foreach(f=>println(f._2))
        val rddEsRes = esLines.reduceByKey((valueMap0, valueMap1) => StreamProcess.mergeDataStatis(valueMap0, valueMap1))
//          rddEsRes.collect().foreach(f=>println(f._2))
//               println("rddEsRes====>"+rddEsRes.take(1).apply(0)._2+"++++++++"+rddEsRes.take(1).apply(0)._1)
//                 println("rddHbaseRes====>"+rddRes.take(1).apply(0)._2+"+++rddResKey+++++"+rddRes.take(1).apply(0)._1)

        val rddjoin = rddRes.leftOuterJoin(rddEsRes)
        val mergedRdd = rddjoin.map(f => {
          val allres = f._2
          val hbaseBand = allres._1
          println("hbaseBand===>" + hbaseBand)
          val sparkBand = allres._2
          println("sparkBand===>" + sparkBand)
          val mergedValue = BandCheckProcess.mergeBandData(hbaseBand, sparkBand)
          mergedValue
        })
          mergedRdd.saveToEs("{index_name}/{index_type}", Map("es.mapping.exclude" -> "index_name,index_type"))
        
      }
    }

    // =================================
    sc.stop()
  }
  def getLogTimeRange(startTime: Long, endTime: Long): Array[Tuple7[String, String, String, String, String, Long, Long]] = {
    //一次分析时间间隔,5分钟的倍数
    val arrBuffer = ArrayBuffer[Tuple7[String, String, String, String, String, Long, Long]]()
    var timeBegin = startTime
    var timeEnd = startTime + INTEAL
    while (timeEnd.<=(endTime)) {
      val strBegin = DateFormatBBD.convertToStrYMDHM(timeBegin)
      val strEnd = DateFormatBBD.convertToStrYMDHM(timeEnd)
      val tablesubfix = DateFormatBBD.convertToStrYMDNODot(timeBegin)
      val lagTableSubfix = DateFormatBBD.convertToStrYM_NoDot(timeBegin)
      val tableName = hbaseTablePrefix + tablesubfix
      val tableLagName = hbaseTablePrefix + lagTableSubfix
      //
      val timeUnixBegin = timeBegin / 1000
      val timeUnixEnd = timeEnd / 1000
      val indexFix = DateFormatBBD.convertToStr(timeUnixBegin)
      val indexEsName = esIndex + indexFix

      val range = new Tuple7(tableName, tableLagName, strBegin, strEnd, indexEsName, timeUnixBegin, timeUnixEnd)
      arrBuffer.append(range)
      timeBegin = timeEnd
      timeEnd = timeBegin + INTEAL
    }
    if (timeEnd > endTime && timeBegin < endTime) {
      val strBegin = DateFormatBBD.convertToStrYMDHM(timeBegin)
      val strEnd = DateFormatBBD.convertToStrYMDHM(endTime)
      val tablesubfix = DateFormatBBD.convertToStrYMDNODot(timeBegin)
      val lagTableSubfix = DateFormatBBD.convertToStrYM_NoDot(timeBegin)
      //nginx20170602
      val tableName = hbaseTablePrefix + tablesubfix
      val tableLagName = hbaseTablePrefix + lagTableSubfix
      //
      val timeUnixBegin = timeBegin / 1000
      val timeUnixEnd = endTime / 1000
      val indexFix = DateFormatBBD.convertToStr(timeUnixBegin)
      val indexEsName = esIndex + indexFix
      val range = new Tuple7(tableName, tableLagName, strBegin, strEnd, indexEsName, timeUnixBegin, timeUnixEnd)
      arrBuffer.append(range)
    }

    arrBuffer.toArray

  }

}