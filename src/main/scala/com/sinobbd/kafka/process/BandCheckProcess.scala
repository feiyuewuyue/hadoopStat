package com.sinobbd.kafka.process

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import org.apache.commons.codec.digest.DigestUtils
import com.sinobbd.strider.dota.DateFormatBBD

object BandCheckProcess {
  def main(args1: Array[String]) {
//    val sbKey = new StringBuffer();
//    val mergeKey = Array("file_name", "from_time")
//    val num: Double = 1.4939145E9
//    val resValue = new scala.collection.mutable.HashMap[String, Any];
//    resValue.put("from_time", num)
//    resValue.put("file_name", "www")
//    // for (key <- mergeKey) {   
//    //      sbKey.append(resValue.get(key).getOrElse(0).toString());
//    //    }
//    val fileName = resValue.get("file_name").getOrElse(0).toString()
//    val fromTime = resValue.get("from_time").getOrElse(0).toString().toDouble.toLong
//    sbKey.append(fileName);
//    sbKey.append(fromTime);
    
    val fileName="1.49417424E9"
    val fileMd5= fileName.toDouble.toLong
    println(fileMd5.toString())
  }

  /**
   * 将明细日志转为带宽记录数据，不进行合并
   */
  def transDetailLogToFileTraffic(hmDetail: Map[String, Any]): Tuple2[String, Map[String, Any]] = {
    val listNormal = ListBuffer[Map[String, Any]]()
    StreamProcess.processAbnormalDataForBand(hmDetail)
    val hm5mStats = transToTuple5m(hmDetail)
    hm5mStats
  }
  
  def transRowkeyToFileInfo(rowkey:String): Tuple2[String, Map[String, Any]] = {
    //0#!yyyyMMddHHmm
    //0#!201705090000#!BS#35153d7bb13db62c682b84e1d56a4e80#1190
    val strArray= rowkey.split("#!")
    val strDateTime=strArray.apply(1)
    val strOther=strArray.apply(2)
    val fileArray=strOther.split("#")
    val fileMd5=fileArray.apply(1)
    val value = new scala.collection.mutable.HashMap[String, Any];
    val time_unix=DateFormatBBD.convertNoDotToDateTimeMin(strDateTime)
    val time = (time_unix.getMillis/1000/300)*300    
    value.put(FieldConst.field_es_fromtime, time)
    value.put(FieldConst.field_es_req_count, 1);
    value.put(FieldConst.field_file_name, fileMd5);
   val key = getJoinKeyValue(fileMd5, time)
     val res = new Tuple2[String, Map[String, Any]](key, value);
    res;
     
  }
  def transBandToTuple5m(logDetail: Map[String, Any]): Tuple2[String, Map[String, Any]] = {
    //Key赋值 
    val mergeKey = Array("file_name", "from_time")
    val fileName = logDetail.get("file_name").getOrElse(0).toString()
    val fromTime = logDetail.get("from_time").getOrElse(0).toString().toDouble.toLong
    val time = (fromTime / 300) * 300
    val fileMd5 = DigestUtils.md5Hex(fileName)
    val key = getJoinKeyValue(fileMd5, time)
    val res = new Tuple2[String, Map[String, Any]](key, logDetail);
    res;
  }

  def getJoinKeyValue(fileName: String, fromTime: Long): String = {
    val sbKey = new StringBuffer();
    sbKey.append(fileName)    
    sbKey.append("_")
    sbKey.append(fromTime.toString())    
  //  val skey = DigestUtils.md5Hex(sbKey.toString())
    sbKey.toString()
  }

  def transToTuple5m(logDetail: Map[String, Any]): Tuple2[String, Map[String, Any]] = {
    val value = new scala.collection.mutable.HashMap[String, Any];
    val time_ori = logDetail.get(FieldConst.field_time_unix).getOrElse(0).toString().toDouble.toLong

    val fileName = logDetail.get("file_name").getOrElse(0).toString()
    value.put(FieldConst.field_file_name, fileName);
    //赋值Value
    value.put(FieldConst.field_es_req_count, 1);
    value.put(FieldConst.field_es_size, logDetail.get(FieldConst.field_size).getOrElse(0));

    // 取出的时间为秒，将时间转换为整分
    val time = (time_ori / 300) * 300
    value.put(FieldConst.field_es_fromtime, time)

    val key = getJoinKeyValue(fileName, time)

    value.put(FieldConst.field_es_timestamp, System.currentTimeMillis())

    //设置主健初始值(id和offset组合)，保存前在主健前拼接批次号，以解决多次录入问题
    //    value.put(FieldConst.field_id, key)
    //    value.put(FieldConst.field_es_kafkaoffset, logDetail.get(FieldConst.field_es_kafkaoffset).getOrElse("0"))
    val res = new Tuple2[String, Map[String, Any]](key, value);
    res;
  }
  def mergeBandData(hbaseBand: Map[String, Any], sparkBand: Option[Map[String, Any]]): Map[String, Any] = {
    val resValue = new scala.collection.mutable.HashMap[String, Any];

   // val eslog_traffic = hbaseBand.get(FieldConst.field_es_size).getOrElse(0).toString().toDouble
    val esReqCount = hbaseBand.get(FieldConst.field_es_req_count).getOrElse(0).toString().toDouble
   // resValue.put("eslog_traffic", eslog_traffic)
    resValue.put("es_req_count", esReqCount)

    resValue.put("time_begin", hbaseBand.get(FieldConst.field_es_fromtime))
    resValue.put(FieldConst.field_file_name, hbaseBand.get(FieldConst.field_file_name))
    val fromTime = hbaseBand.get(FieldConst.field_es_fromtime).getOrElse(0).toString().toLong
    resValue.put(FieldConst.field_es_logdate, fromTime * 1000)
//    resValue.put(FieldConst.field_es_req_count, hbaseBand.get(FieldConst.field_es_req_count))
    resValue.put("is_normal", "N")

    resValue.put("index_name", "log-band-checktest")
    resValue.put("index_type", "task")

    if (!sparkBand.isEmpty) {
      val sparkBandValue = sparkBand.get
      val sparkTraffic = sparkBandValue.get(FieldConst.field_es_size).getOrElse(0).toString().toDouble
      val sparkReqCount = sparkBandValue.get(FieldConst.field_es_req_count).getOrElse(0).toString().toDouble
      val fileName= sparkBandValue.get(FieldConst.field_file_name).getOrElse(0).toString()
      resValue.put(FieldConst.field_file_name, fileName)
      //resValue.put("spark_traffic", sparkTraffic)
      resValue.put("spark_req_count", sparkReqCount)
      if ( esReqCount.equals(sparkReqCount)) {
        resValue.put("is_normal", "Y")
      }

    }
    resValue
  }
}