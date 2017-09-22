package org.apache.spark.streaming.kafka

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.spark.SparkConf


object sparkOnHbase {
  
  
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
  
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("sparkOnHbase")
     val sc = new SparkContext()

    val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181");
	    conf.set("hbase.rpc.timeout", "1800000");
	    conf.set("hbase.client.scanner.timeout.period", "1800000");
	    conf.set("mapreduce.task.timeout", "1800000");
	    conf.set(TableInputFormat.INPUT_TABLE, args(0))
    //添加过滤条件，年龄大于 18 岁
    val scan = new Scan()
	    
	  val comp = new SubstringComparator(args(1));
	  val filter1=new RowFilter(CompareFilter.CompareOp.EQUAL,
                comp);  
	  
    scan.setFilter(filter1)
    scan.addColumn("cf".getBytes, "info1".getBytes)
    scan.addColumn("cf".getBytes, "info".getBytes)
    conf.set(TableInputFormat.SCAN,convertScanToString(scan))

    val usersRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
     
   // val count = usersRDD.count()
   // println("Users RDD Count:" + count)
   // usersRDD.cache()
   usersRDD.map(x=>(x._2.getValue("cf".getBytes,"info1".getBytes).toString().split("\" \"")(0),1)).reduceByKey(_ + _).collect().foreach(println)
    //usersRDD.reduceByKey(func)
    //遍历输出
 /*   usersRDD.foreach{ case (_,result) =>
      val key = Bytes.toInt(result.getRow)
      val name = Bytes.toString(result.getValue("cf".getBytes,"info1".getBytes))
   //   val age = Bytes.toInt(result.getValue("basic".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name)
    }*/
  }
}