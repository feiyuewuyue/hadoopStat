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
/**
 * 从hbase取数据计算带宽
 */
object BandwidthHbase {
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
    var esNodes = "183.131.54.181"
    if (args.length >= 3) {
      esNodes = args.apply(2)
    }
    val timeBegin = DateFormatBBD.convertYMDHMtoMill(fromTimeWhole)
    val timeEnd = DateFormatBBD.convertYMDHMtoMill(toTimeWhole)
    val arrayStr = getLogTimeRange(timeBegin, timeEnd)

    val sparkConf = new SparkConf().setAppName("BandwidthOnHbase") //.setMaster("local")
    //   System.setProperty("hadoop.home.dir", "D:\\hadoop");
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
      val tableName = param._1 //"jx20170405"
      val fromTime = param._2 // "2017040500"
      val toTime = param._3 //"2017040501"
      print("====tableName====" + tableName)
      print("====fromTime====" + fromTime)
      print("====toTime====" + toTime)

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
      val lines = usersRDD.flatMap(f => {
        val result = f._2
        val userlog = Bytes.toString(result.getValue(column_family.getBytes, column_log.getBytes))
        val otherlog = Bytes.toString(result.getValue(column_family.getBytes, column_other.getBytes))
        val hmValue = BusinessProcess.parseJXLog(userlog, otherlog)
        val res = StreamProcess.transDetailLogToJXBand(hmValue)
        res

      })

      // println(lines.count())
      //将数据转换为key-value形式，并在同一个RDD内汇总StreamProcess.mergeDetailLogJX(f)
      //      val rddK = lines.mapPartitions(f => { StreamProcess.mergeDetailLogJX(f) }, true)
      //    System.out.print(rddK.take(3))
      val rddRes = lines.reduceByKey((valueMap0, valueMap1) => StreamProcess.mergeDataStatis(valueMap0, valueMap1))
      val rddPersist = rddRes.map(f => f._2)
      if (!rddPersist.isEmpty) {
        //      System.out.println(rddPersist.take(3))
        //      rddPersist.foreach(print(_))
        val rddSave = rddPersist.map(f => { StreamProcess.setAndModifyID(f) })
        rddSave.saveToEs("{index_name}/{index_type}", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "id,index_name,index_type"))
      }
    }
    // =================================
    sc.stop()
  }
  def getLogTimeRange(startTime: Long, endTime: Long): Array[Tuple3[String, String, String]] = {
    //一次分析时间间隔
    val INTEAL = 600 * 1000
    val arrBuffer = ArrayBuffer[Tuple3[String, String, String]]()
    var timeBegin = startTime
    var timeEnd = startTime + INTEAL
    while (timeEnd.<=(endTime)) {
      val strBegin = DateFormatBBD.convertToStrYMDHM(timeBegin)
      val strEnd = DateFormatBBD.convertToStrYMDHM(timeEnd)
      val tablesubfix = DateFormatBBD.convertToStrYMDNODot(timeBegin)
      val tableName = "jx" + tablesubfix
      val range = new Tuple3(tableName, strBegin, strEnd)
      arrBuffer.append(range)
      timeBegin = timeEnd
      timeEnd = timeBegin + INTEAL
    }
    if (timeEnd > endTime && timeBegin < endTime) {
      val strBegin = DateFormatBBD.convertToStrYMDHM(timeBegin)
      val strEnd = DateFormatBBD.convertToStrYMDHM(endTime)
      val tablesubfix = DateFormatBBD.convertToStrYMDNODot(timeBegin)
      val tableName = "jx" + tablesubfix
      val range = new Tuple3(tableName, strBegin, strEnd)
      arrBuffer.append(range)
    }

    arrBuffer.toArray

  }
}
