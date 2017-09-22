package org.apache.spark.streaming.kafka


import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka._
import org.elasticsearch.spark.sql._

/**
  * Created by zhang on 2017/8/21 0021.
  */
object ReadFileToEs {

  def logHandler(line: String): LogpackageJXLog = {
    val array = line.split("\" \"")
    /*println("=============================================")
    println(array(9),array(21),array(32),array(3),line,
      array(9)+ "-" + array(21))*/
    return LogpackageJXLog(array(9),array(21),array(32),array(3),line,
      array(9)+ "-" + array(21), "zz_test_filetoes", "jx20170822")
  }

  def main(args: Array[String]): Unit = {

    /*val tableName = args(0)
    val family = args(1)
    val column = args(2)

    val column2 = args(7)

    val output = args(3)
    val date = args(4)
    val hour = args(5)
    val range = "!"+date+hour
    //    val hostname = args(6)
    val hostname = args(6).split(",")*/

    val filepath = args(0)
    val esNodes = args(1)
    val esPort = args(2)

//    val filepath = "D:\\study\\code\\sinobbd\\project\\hadoopStats\\folder\\logpackagelog\\20170822"
//    val esNodes = "183.131.54.181"
//    val esPort = "9200"
    val zookeeper = "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181"

    val sparkConf = new SparkConf().setAppName("readFileToEs")//.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    sparkConf.set("es.nodes", esNodes).set("es.port", esPort)
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.batch.size.bytes", "450mb")
    sparkConf.set("es.batch.size.entries", "25000")
    sparkConf.set("es.batch.write.refresh", "false")
    sparkConf.set(" es.batch.write.retry.count", "3")
    //  sparkConf.set("es.input.max.docs.per.partition","200000")
    sparkConf.set("fetch.message.max.bytes", "5m")

    /*val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", zookeeper)
    config.set("hbase.rpc.timeout", "1800000")
    config.set("hbase.client.scanner.timeout.period", "1800000")
    val jx_scan = new Scan()
    val jx_filter = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    jx_scan.setCaching(500)
    jx_scan.setCacheBlocks(false)
    jx_scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
    jx_scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column2))
    val comp = new SubstringComparator(range)
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, comp) //日志时间
    //    val jx_filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("#@4")) //layer=4
    jx_filter.addFilter(filter)
    //    jx_filter.addFilter(jx_filter2)
    jx_scan.setFilter(jx_filter)

    //处理单个hbase表
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    config.set(TableInputFormat.SCAN, convertScanToString(jx_scan))
    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(config,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //输出日志
    val sqlContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._
    hbaseRdd.map(x => {
      val info1 = Bytes.toString(x._2.getValue("cf".getBytes(), "info1".getBytes()))//打包字段
      val info2 = Bytes.toString(x._2.getValue("cf".getBytes(), "info".getBytes()))
      val hostn = info1.split("\" \"")(24)
      ( hostn, info1 + "<<>>" + info2)
    }).filter(x => hostname.contains(x._1)).map(line => logHandler(line._2)).toDF.saveToEs("{index_name}/{index_type}", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "index_name,index_type"))
    */

    val rdds = sc.textFile(filepath)
    val sqlContext = SQLContextSingleton.getInstance(rdds.sparkContext)
//    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sc.textFile(filepath).map(line => logHandler(line)).toDF.saveToEs("{index_name}/{index_type}", Map("es.mapping.id" -> "id", "es.mapping.exclude" -> "index_name,index_type"))
    /*sc.textFile(filepath).map(line => {
      println("====>>>")
      logHandler(line)
    })*/

    sc.stop()
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

}

case class LogpackageJXLog(X_Info_request_id: String, layer: String, time_local: String, time_unix: String, body: String,
                            id: String, index_name: String, index_type: String)
