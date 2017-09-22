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

object FusionBandHbase {
  val column_family = "cf"
  val column_log = "info"
  val column_other = "info1"
  //  val esNodes = BasicSettingReader.configration.get("es.nodes.jx").toString()
  //  val esPort = BasicSettingReader.configration.get("es.port.jx").toString()

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {
    if (!args.isEmpty) {
      for (key <- args) {
        print("====args====" + key)
      }
    }
    if (args == null || args.length < 2) {
      print("请输入参数：1：开始时间(例如：2017040500) 2：结束时间 3：ES Client IP")
      return
    }
    val fromTimeWhole = args.apply(0) // "2017040500"
    val toTimeWhole = args.apply(1) //"2017040501"
    var esNodes = args.apply(2)//"183.131.54.181"
    var firmName: String = "all"
    if (args.length >= 4) {
      firmName = args.apply(3)
    }
    val timeBegin = DateFormatBBD.convertYMDHMtoMill(fromTimeWhole)
    val timeEnd = DateFormatBBD.convertYMDHMtoMill(toTimeWhole)
    val arrayStr = getLogTimeRange(timeBegin, timeEnd)

    val sparkConf = new SparkConf().setAppName("BandwidthOnHbase") //.setMaster("local")
    //   System.setProperty("hadoop.home.dir", "D:\\hadoop");
    sparkConf.set("spark.executor.userClassPathFirst", "true");
    sparkConf.set("spark.driver.userClassPathFirst", "true");
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.batch.size.bytes", "100mb")
    sparkConf.set("es.batch.size.entries", "10000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set("es.nodes", esNodes) //183.136.128.47:9200
    sparkConf.set("es.port", "9200")

    val sc = new SparkContext(sparkConf)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "183.131.54.142,183.131.54.143,183.131.54.144")

    for (param <- arrayStr) {
      val tableName = param._1 //"nginx20170602"
      val tableLagName = param._2 //"nginx201706"
      val fromTime = param._3 // "2017040500"
      val toTime = param._4 //"2017040501"
      val scan = new Scan()
      //       val jarFilePath = scan.getClass.getProtectionDomain().getCodeSource().getLocation().getFile();
      //      println("===jarFilePath==="+jarFilePath+"=============")
      print("====tableName====" + tableName)
      print("====fromTime====" + fromTime)
      print("====toTime====" + toTime)
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

      scan.setCaching(1000);
      scan.setCacheBlocks(false);
      scan.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(column_log));
      scan.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(column_other));
      hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))
      hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
      val usersRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
      hbaseConf.set(TableInputFormat.INPUT_TABLE, tableLagName)
      val usersLagRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

      val userLogRdd = usersRDD.union(usersLagRDD)
      val lines = userLogRdd.flatMap(f => {
        val result = f._2
        val userlog = Bytes.toString(result.getValue(column_family.getBytes, column_log.getBytes))
        val otherlog = Bytes.toString(result.getValue(column_family.getBytes, column_other.getBytes))
        val hmValue = BusinessProcess.parseFusionLog(userlog, otherlog)
        val res = StreamProcess.transDetailLogToFusionBand(hmValue)
        res

      })

      // println(lines.count())
      //将数据转换为key-value形式，并在同一个RDD内汇总StreamProcess.mergeDetailLogJX(f)
      //      val rddK = lines.mapPartitions(f => { StreamProcess.mergeDetailLogFusionForBand(f) }, true)
      //    System.out.print(rddK.take(3))
      val rddRes = lines.reduceByKey((valueMap0, valueMap1) => StreamProcess.mergeDataStatis(valueMap0, valueMap1))
      val rddPersist = rddRes.map(f => f._2)
      if (!rddPersist.isEmpty) {
        //      System.out.println(rddPersist.take(3))
        //      rddPersist.foreach(print(_))
        val rddSave = rddPersist.map(f => { StreamProcess.setAndModifyID(f) })
        rddSave.saveToEs("{index_name}/{index_type}", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "id,index_name,index_type,offset"))
      }
    }
    // =================================
    sc.stop()
  }
  def getLogTimeRange(startTime: Long, endTime: Long): Array[Tuple4[String, String, String, String]] = {
    //一次分析时间间隔
    val INTEAL = 20*60 * 1000
    val arrBuffer = ArrayBuffer[Tuple4[String, String, String, String]]()
    var timeBegin = startTime
    var timeEnd = startTime + INTEAL
    while (timeEnd.<=(endTime)) {
      val strBegin = DateFormatBBD.convertToStrYMDHM(timeBegin)
      val strEnd = DateFormatBBD.convertToStrYMDHM(timeEnd)
      val tablesubfix = DateFormatBBD.convertToStrYMDNODot(timeBegin)
      val lagTableSubfix = DateFormatBBD.convertToStrYM_NoDot(timeBegin)
      val tableName = "nginx" + tablesubfix
      val tableLagName = "nginx" + lagTableSubfix
      val range = new Tuple4(tableName, tableLagName, strBegin, strEnd)
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
      val tableName = "nginx" + tablesubfix
      val tableLagName = "nginx" + lagTableSubfix
      val range = new Tuple4(tableName, tableLagName, strBegin, strEnd)
      arrBuffer.append(range)
    }

    arrBuffer.toArray

  }
}