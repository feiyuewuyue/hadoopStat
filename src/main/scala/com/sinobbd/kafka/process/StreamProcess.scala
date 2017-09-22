package com.sinobbd.kafka.process

import java.util.ArrayList
import java.util.List

import scala.collection.Iterator
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

import org.apache.log4j.Logger

import net.sf.json.JSONObject

import com.sinobbd.data.merge.ParseSrcData

import scala.collection.JavaConverters

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
import org.json4s.DateFormat
import com.sinobbd.strider.dota.DateFormatBBD
import java.util.List
import scala.util.Random
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.commons.codec.digest.DigestUtils
import java.rmi.server.ObjID
import com.sinobbd.tercelsrear.urlrank.DateISO

object StreamProcess {
  val LOG = Logger.getLogger("StreamProcess");
  val Index_pre = "nginx-zss-"
  val Index_jx_pre = "nginx-"
  val Index_type_detail = "fluentd"
  val Index_type_Fusion_band = "bandwidth"
  val Index_type_JXband = "bandself"
  val statis_1m_jx_pre = "statsdata_m_"
  val statis_5m_jx_pre = "statsdata_5m_"
  val statis_1m_fusion_pre = "statsdata_m_"
  val statis_5m_fusion_pre = "statsdata_5m_"
  val statis_host_jx_pre = "stats-host-"

  def main(args1: Array[String]) {

    test()
  }

  def test() {
//     val num0: Double = valueMap0.get(FieldConst.field_es_req_count).getOrElse(0).toString().toDouble
//    val num1: Double = valueMap1.get(FieldConst.field_es_req_count).getOrElse(0).toString().toDouble
//
//    val size0: Double = valueMap0.get(FieldConst.field_es_size).getOrElse(0).toString().toDouble
//    val size1: Double = valueMap1.get(FieldConst.field_es_size).getOrElse(0).toString().toDouble
//    val sum: Double = size0 + size1;
//    val numCount: Double = num0 + num1
//    valueMap0.put(FieldConst.field_es_req_count, numCount);
//    valueMap0.put(FieldConst.field_es_size, sum);
//    valueMap0.put(FieldConst.field_es_size_pm, sum);
//    //保留最小的offset
//    val offset0=valueMap0.get(FieldConst.field_es_kafkaoffset).getOrElse("0").toString()
//    val offset1=valueMap1.get(FieldConst.field_es_kafkaoffset).getOrElse("0").toString()
//    var offsetMin=offset0
//    if(offsetMin.compareTo(offset1)>0){
//      offsetMin=offset1
//    }
//    valueMap0.put(FieldConst.field_es_kafkaoffset, offsetMin); 
//    valueMap0;
    
    
    val valueMap0=Map(FieldConst.field_es_req_count->"1",FieldConst.field_es_size->"2",FieldConst.field_es_kafkaoffset->"0-3")
    val valueMap1=Map(FieldConst.field_es_req_count->"1",FieldConst.field_es_size->"2",FieldConst.field_es_kafkaoffset->"0-2")
   val res= mergeDataStatis(valueMap0.asInstanceOf[Map[String, Any]], valueMap1.asInstanceOf[Map[String, Any]])

    print(res)
  }

  /**
   * 返回多个Map，每个为一种统计结果，比如有两个，分别为按1分钟和5分钟统计结果
   */
  def getFusionDetailLog(valueIter: Iterator[Map[String, Any]]): Iterator[Map[String, Any]] = {
    if (valueIter == Nil || valueIter.isEmpty) {
      return valueIter
    }
    //val form = DateFormat.getSimpleDateFormat();
    val listNormal = ListBuffer[Map[String, Any]]()
    while (valueIter.hasNext) {
      val hmValue = valueIter.next();
      processAbnormalDataForDetail(hmValue)
      setFusionRowid(hmValue)
      setFusionDetailIndexAndType(hmValue)
      listNormal.append(hmValue)
    }
    val res = listNormal.iterator

    return res;
  }
  /**
   * 返回多个Map
   */
  def getJXDetailLog(valueIter: Iterator[Map[String, Any]]): Iterator[Map[String, Any]] = {
    if (valueIter == Nil || valueIter.isEmpty) {
      return valueIter
    }
    //val form = DateFormat.getSimpleDateFormat();
    val listNormal = ListBuffer[Map[String, Any]]()
    val listAbNormal = ListBuffer[Map[String, Any]]()
    while (valueIter.hasNext) {
      val hmValue = valueIter.next();
      processAbnormalDataForDetail(hmValue)
      setJXRowid(hmValue)
      setJXDetailIndexAndType(hmValue)
      listNormal.append(hmValue)
    }
    val res = listNormal.iterator

    return res;
  }
  def setJXDetailIndexAndType(tp1m: Map[String, Any]): Map[String, Any] = {
    val timeUnix = tp1m.get(FieldConst.field_time_unix).getOrElse(0).toString()
    val sdate = DateFormatBBD.convertToStrYM(timeUnix)
    val firmName = tp1m.get(FieldConst.field_firm_name).getOrElse(0).toString().toLowerCase()
    if (sdate != null) {
      val index_name = Index_jx_pre + firmName + "-" + sdate
      tp1m.put("index_name", index_name)
      tp1m.put("index_type", Index_type_detail)

    }

    tp1m
  }

  def setFusionDetailIndexAndType(tp1m: Map[String, Any]): Map[String, Any] = {
    val fileName = tp1m.get(FieldConst.field_file_name).getOrElse(0).toString()
    val firmName = tp1m.get(FieldConst.field_firm_name).getOrElse(0).toString().toLowerCase()
    if (fileName != null) {
      val fileArray = fileName.split("_");
      if (fileArray.length > 3) {
        val timeFile = fileArray.apply(2)
        if (timeFile.length() > 8) {
          val _rm_fileTime = timeFile.substring(0, 8);
          val index_name = Index_pre + firmName + "-" + _rm_fileTime
          tp1m.put("index_name", index_name)
          tp1m.put("index_type", Index_type_detail)
        }
      }

      // val sdate = DateFormatBBD.convertToStr(dtime)
    }

    tp1m
  }

  /**
   * 将明细日志转为带宽记录数据，不进行合并
   */
  def transDetailLogToFusionBand(hmDetail: Map[String, Any]): Iterator[Tuple2[String, Map[String, Any]]] = {   
    val listNormal = ListBuffer[Map[String, Any]]()   
      processAbnormalDataForBand(hmDetail) 
    val res = getAllDatasFusionSingle(hmDetail)
    res
  }
  
   def transDetailLogToJXBand(hmDetail: Map[String, Any]): Iterator[Tuple2[String, Map[String, Any]]] = {   
    val listNormal = ListBuffer[Map[String, Any]]()   
      processAbnormalDataForBand(hmDetail) 
    val res = getAllDatasJXSingle(hmDetail)
    res
  }
    @deprecated
   def mergeDetailLogFusionForBand(valueIter: Iterator[Map[String, Any]]): Iterator[Tuple2[String, Map[String, Any]]] = {   
    val listNormal = ListBuffer[Map[String, Any]]()
    while (valueIter.hasNext) {
      val hmValue = valueIter.next();
      processAbnormalDataForBand(hmValue)
      listNormal.append(hmValue)
    }
    val normalLog= listNormal.iterator;
    val res = getAllDatasFusion(normalLog)
    res
  }
//  @deprecated
//  def mergeDetailLogFusion(valueIter: Iterator[Map[String, Any]]): Iterator[Tuple2[String, Map[String, Any]]] = {
//    val normalLog = processDetailLogCom(valueIter) //Index_type_Fusion_band
//    val res = getAllDatasFusion(normalLog)
//    res
//  }

//  def mergeDetailLogJX(valueIter: Iterator[Map[String, Any]]): Iterator[Tuple2[String, Map[String, Any]]] = {
//    val normalLog = processDetailLogCom(valueIter)
//    val res = getAllDatasJX(normalLog)
//    res
//  }

  /**
   * 返回多个Map，每个为一种统计结果，比如有两个，分别为按1分钟和5分钟统计结果
   */
 // @deprecated
//  def processDetailLogCom(valueIter: Iterator[Map[String, Any]]): Iterator[Map[String, Any]] = {
//    if (valueIter == Nil || valueIter.isEmpty) {
//      System.out.println("=====mergeDetailLogCom=====valueIter值为空")
//      return valueIter
//    }
//    val listNormal = ListBuffer[Map[String, Any]]()
//    while (valueIter.hasNext) {
//      val hmValue = valueIter.next();
//      processAbnormalDataForDetail(hmValue)
//      listNormal.append(hmValue)
//    }
//    return listNormal.iterator;
//  }
  
   def getAllDatasFusionSingle(hmNormal: Map[String, Any]): Iterator[(String, Map[String, Any])] = {
    val res = ListBuffer[Tuple2[String, Map[String, Any]]]()
   
     val hm1mStats = transToTuple1m(hmNormal, FieldConst.keyStrs_xm_stat_fusion)
     setIndexAndType(hm1mStats, statis_1m_fusion_pre, Index_type_Fusion_band)
     
     val hm5mStats = transToTuple5m(hm1mStats, FieldConst.keyStrs_xm_stat_fusion)
      setIndexAndType(hm5mStats, statis_5m_fusion_pre, Index_type_Fusion_band)
      res.append(hm1mStats)
      res.append(hm5mStats)

    res.iterator
  }
   
    def getAllDatasJXSingle(hmNormal: Map[String, Any]): Iterator[(String, Map[String, Any])] = {
    val res = ListBuffer[Tuple2[String, Map[String, Any]]]()
      val hm1mStats = transToTuple1m(hmNormal, FieldConst.keyStrs_xm_stat_jx)
     setIndexAndType(hm1mStats, statis_1m_jx_pre, Index_type_JXband)
    
      val hm5mStats = transToTuple5m(hm1mStats, FieldConst.keyStrs_xm_stat_jx)
      setIndexAndType(hm5mStats, statis_5m_jx_pre, Index_type_JXband)
      
        val stathost = transToTupleHost(hmNormal, FieldConst.keyStrs_xm_jx_host)
      setIndexAndTypeMnth(stathost, statis_host_jx_pre, Index_type_JXband)
      
      
      res.append(hm1mStats)
      res.append(hm5mStats)
      res.append(stathost)
      
       res.iterator
    
  }
   @deprecated
  def getAllDatasFusion(listNormal: Iterator[Map[String, Any]]): Iterator[(String, Map[String, Any])] = {
    val res = ListBuffer[Tuple2[String, Map[String, Any]]]()
    if (!listNormal.isEmpty) {
      val hm1mStats = get1mStats(listNormal, FieldConst.keyStrs_xm_stat_fusion)
      val iter = hm1mStats.duplicate
      val hm5mStats = get5mStats(iter._1, FieldConst.keyStrs_xm_stat_fusion)
      for (stat1m <- iter._2) {
        val elems = setIndexAndType(stat1m, statis_1m_fusion_pre, Index_type_Fusion_band)
        res.append(elems)
      }
      for (stat5m <- hm5mStats) {
        val elems = setIndexAndType(stat5m, statis_5m_fusion_pre, Index_type_Fusion_band)
        res.append(elems)
      }
    } else {
      System.out.println("=====getAllDatasFusion=====valueIter值为空")
    }

    res.iterator
  }

  def getAllDatasJX(listNormal: Iterator[Map[String, Any]]): Iterator[(String, Map[String, Any])] = {
    val res = ListBuffer[Tuple2[String, Map[String, Any]]]()
    if (!listNormal.isEmpty) {
      val detail = listNormal.duplicate
      val hm1mStats = get1mStats(detail._1, FieldConst.keyStrs_xm_stat_jx)
      val iter = hm1mStats.duplicate
      val hm5mStats = get5mStats(iter._1, FieldConst.keyStrs_xm_stat_jx)
      for (stat1m <- iter._2) {
        val elems = setIndexAndType(stat1m, statis_1m_jx_pre, Index_type_JXband)
        res.append(elems)
      }
      for (stat5m <- hm5mStats) {
        val elems = setIndexAndType(stat5m, statis_5m_jx_pre, Index_type_JXband)
        res.append(elems)
      }
      //按host、status统计数据
      val hmHostStats = getHostStats(detail._2, FieldConst.keyStrs_xm_jx_host)
      for (stathost <- hmHostStats) {
        val elems = setIndexAndTypeMnth(stathost, statis_host_jx_pre, Index_type_JXband)
        res.append(elems)
      }
    }

    res.iterator
  }

  def setIndexAndTypeMnth(tp1m: Tuple2[String, Map[String, Any]], pre: String, indexType: String): Tuple2[String, Map[String, Any]] = {
    val timeUnix = tp1m._2.get(FieldConst.field_es_logdate).getOrElse(0)
    val dtime = timeUnix.toString().toDouble
    val sdate = DateFormatBBD.convertMillsToStrYM(dtime)
    val indexName = pre + sdate
    tp1m._2.put("index_name", indexName)
    tp1m._2.put("index_type", indexType)
    //tp1m._2.put("def2", "spk")
    tp1m
  }

  def setIndexAndType(tp1m: Tuple2[String, Map[String, Any]], pre: String, indexType: String): Tuple2[String, Map[String, Any]] = {
    val timeUnix = tp1m._2.get(FieldConst.field_es_fromtime).getOrElse(0)
    val dtime = timeUnix.toString().toDouble
    val sdate = DateFormatBBD.convertToStr(dtime)
    //如果是1个月以前的索引，则放到error表中
    val currentTime=System.currentTimeMillis()/1000
     var indexName = pre + sdate
    if((currentTime-dtime)>2592000){
      indexName=pre + "error"
    }  
    tp1m._2.put("index_name", indexName)
    tp1m._2.put("index_type", indexType)
    //tp1m._2.put("def2", "spk")
    tp1m
  }

  def setJXRowid(hm: Map[String, Any]) {
    val request_id = hm.get(FieldConst.field_X_Info_request_id).getOrElse("0").toString()
    val layer = hm.get(FieldConst.field_layer).getOrElse("0").toString()
    val id = request_id + "-" + layer //+ Random.nextInt()
    hm.put("checkd", id)
    hm.put("id", id)

  }

  def setFusionRowid(hm: Map[String, Any]) {
    val fileName = hm.get(FieldConst.field_file_name).getOrElse("0").toString()
    val lineNum = hm.get(FieldConst.field_line_num).getOrElse("0").toString()
    val id = fileName + "-" + lineNum
    hm.put("id", id)
  }

  def processAbnormalDataForBand(hm: Map[String, Any]): Boolean = {

    val timeunix = hm.get(FieldConst.field_time_unix).getOrElse(0)
    val body_bytes_sent = hm.get(FieldConst.field_size).getOrElse(0)
//    val http_status = hm.get(FieldConst.field_http_status).getOrElse(0)    
    try {
      val dtime = timeunix.toString().toDouble
     // val time_ori = dtime.toLong     
      val dsize = body_bytes_sent.toString().toDouble
//      val status = http_status.toString().toInt    
      hm.put(FieldConst.field_time_unix, dtime)     
      hm.put(FieldConst.field_size, dsize)
//      hm.put(FieldConst.field_http_status, status)     
      hm.put(FieldConst.field_es_timestamp, System.currentTimeMillis())
      
      true
    } catch {
      case e: Exception => {
        hm.put("except_desc", e.getMessage)
        hm.put("rawdata", hm.toString())
        false
      }
    }

  }

  def processAbnormalDataForDetail(hm: Map[String, Any]): Boolean = {

    val timeunix = hm.get(FieldConst.field_time_unix).getOrElse(0)
    val body_bytes_sent = hm.get(FieldConst.field_size).getOrElse(0)
    val http_status = hm.get(FieldConst.field_http_status).getOrElse(0)
    val response_time = hm.get(FieldConst.field_response_time).getOrElse(0)
    val p_response_time = hm.get(FieldConst.field_p_response_time).getOrElse(0)
    val p_response_length = hm.get(FieldConst.field_p_response_length).getOrElse(0)
    try {
      val dtime = timeunix.toString().toDouble
      val time_ori = dtime.toLong
      val time_min = (time_ori / 60) * 60

      val dsize = body_bytes_sent.toString().toDouble
      val status = http_status.toString().toInt
      var dresponse_time: Double = 0
      if (!response_time.toString().equals("-")) {
        dresponse_time = response_time.toString().toDouble
      }
      var dp_response_time: Double = 0
      if (!p_response_time.toString().equals("-")) {
        dp_response_time = p_response_time.toString().toDouble
      }
      var dp_response_length: Double = 0
      if (!p_response_length.toString().equals("-")) {
        dp_response_length = p_response_length.toString().toDouble
      }
      hm.put(FieldConst.field_time_unix, dtime)
      hm.put(FieldConst.field_time_mt, time_min)
      hm.put(FieldConst.field_size, dsize)
      hm.put(FieldConst.field_http_status, status)
      hm.put(FieldConst.field_response_time, dresponse_time)
      hm.put(FieldConst.field_p_response_time, dp_response_time)
      hm.put(FieldConst.field_p_response_length, dp_response_length)
      hm.put(FieldConst.field_es_timestamp, System.currentTimeMillis())

      true
    } catch {
      case e: Exception => {
        hm.put("except_desc", e.getMessage)
        hm.put("rawdata", hm.toString())
        false
      }
    }

  }


  def mergeDataStatisTp(tpMap0: Tuple2[String, Map[String, Any]], tpMap1: Tuple2[String, Map[String, Any]]): Tuple2[String, Map[String, Any]] = {

    val valueMap0 = tpMap0._2
    val valueMap1 = tpMap1._2
    mergeDataStatis(valueMap0, valueMap1)
    tpMap0;
  }

  def mergeDataStatis(valueMap0: Map[String, Any], valueMap1: Map[String, Any]): Map[String, Any] = {

    val num0: Double = valueMap0.get(FieldConst.field_es_req_count).getOrElse(0).toString().toDouble
    val num1: Double = valueMap1.get(FieldConst.field_es_req_count).getOrElse(0).toString().toDouble

    val size0: Double = valueMap0.get(FieldConst.field_es_size).getOrElse(0).toString().toDouble
    val size1: Double = valueMap1.get(FieldConst.field_es_size).getOrElse(0).toString().toDouble
    val sum: Double = size0 + size1;
    val numCount: Double = num0 + num1
    valueMap0.put(FieldConst.field_es_req_count, numCount);
    valueMap0.put(FieldConst.field_es_size, sum);
    valueMap0.put(FieldConst.field_es_size_pm, sum);
    //保留最小的offset
    val offset0=valueMap0.get(FieldConst.field_es_kafkaoffset).getOrElse("0").toString()
    val offset1=valueMap1.get(FieldConst.field_es_kafkaoffset).getOrElse("0").toString()
    var offsetMin=offset0
    if(offsetMin.compareTo(offset1)>0){
      offsetMin=offset1
    }
    valueMap0.put(FieldConst.field_es_kafkaoffset, offsetMin); 
    valueMap0;
  }

  def getHostStats(listMap: Iterator[Map[String, Any]], mergeKey: Array[String]): Iterator[(String, Map[String, Any])] = {
    val hmStats = Map[String, Tuple2[String, Map[String, Any]]]()
    // 按一分钟粒度统计数据
    for (hmRow <- listMap) {
      val datas = transToTupleHost(hmRow, mergeKey)
      val hm1mRes = hmStats.get(datas._1);

      if (hm1mRes.isEmpty) {
        hmStats.put(datas._1, datas);
      } else {
        val merged = mergeDataStatisTp(hm1mRes.get, datas);
        hmStats.put(merged._1, merged)
      }

    }
    hmStats.valuesIterator
  }
  
 


  def get1mStats(listMap: Iterator[Map[String, Any]], mergeKey: Array[String]): Iterator[(String, Map[String, Any])] = {
    val hm1mStats = Map[String, Tuple2[String, Map[String, Any]]]()
    // 按一分钟粒度统计数据
    for (hmRow <- listMap) {
      val datas = transToTuple1m(hmRow, mergeKey)
      val hm1mRes = hm1mStats.get(datas._1);

      if (hm1mRes.isEmpty) {
        hm1mStats.put(datas._1, datas);
      } else {
        val merged = mergeDataStatisTp(hm1mRes.get, datas);
        hm1mStats.put(merged._1, merged)
      }

    }
    hm1mStats.valuesIterator
  }

  def get5mStats(listMap: Iterator[(String, Map[String, Any])], mergeKey: Array[String]): Iterator[(String, Map[String, Any])] = {
    val hm5mStats = Map[String, Tuple2[String, Map[String, Any]]]()
    // 按一分钟粒度统计数据
    for (hmRow <- listMap) {

      val datas = transToTuple5m(hmRow, mergeKey)

      val hm5mRes = hm5mStats.get(datas._1);
      if (hm5mRes.isEmpty) {
        hm5mStats.put(datas._1, datas);
      } else {
        val merged = mergeDataStatisTp(hm5mRes.get, datas);
        hm5mStats.put(merged._1, merged)
      }

    }
    hm5mStats.valuesIterator
  }
  def transToTupleHost(logDetail: Map[String, Any], mergeKey: Array[String]): Tuple2[String, Map[String, Any]] = {
    val value = new scala.collection.mutable.HashMap[String, Any];
    val time_ori = logDetail.get(FieldConst.field_time_unix).getOrElse(0).toString().toDouble.toLong

    //Key赋值 设置汇总类型字段，用于和5分钟统一处理
    val sbKey = new StringBuffer(FieldConst.prefix_agg_hoststatus);
    //FieldConst.keyStrs_xm_stat
    for (key <- mergeKey) {
      value.put(key, logDetail.get(key).getOrElse(0));
      sbKey.append(logDetail.get(key).getOrElse(0));
    }
    //赋值Value
    value.put(FieldConst.field_es_req_count, 1);
    value.put(FieldConst.field_es_size, 0);
    // 取出的时间为秒，将时间转换为整分    
    val time = (time_ori / 60) * 60;
    value.put(FieldConst.field_es_logdate, time * 1000)
    value.put(FieldConst.field_es_timestamp, System.currentTimeMillis())
    // 将时间按分钟取整后放到key中
    sbKey.append(time);
    val key = sbKey.toString();
    //设置主健初始值，保存前在主健前拼接批次号，以解决多次录入问题
    value.put(FieldConst.field_id, key)
    value.put(FieldConst.field_es_kafkaoffset, logDetail.get(FieldConst.field_es_kafkaoffset).getOrElse("0"))
    val res = new Tuple2[String, Map[String, Any]](key, value);
    res;
  }
  def transToTuple1m(logDetail: Map[String, Any], mergeKey: Array[String]): Tuple2[String, Map[String, Any]] = {
    val value = new scala.collection.mutable.HashMap[String, Any];
    val time_ori = logDetail.get(FieldConst.field_time_unix).getOrElse(0).toString().toDouble.toLong

    //Key赋值 设置汇总类型字段，用于和5分钟统一处理
    val sbKey = new StringBuffer(FieldConst.prefix_agg_1m);
    //FieldConst.keyStrs_xm_stat
    for (key <- mergeKey) {
      value.put(key, logDetail.get(key).getOrElse(0));
      sbKey.append(logDetail.get(key).getOrElse(0));
    }
    //赋值Value
    value.put(FieldConst.field_es_req_count, 1);
    value.put(FieldConst.field_es_size, logDetail.get(FieldConst.field_size).getOrElse(0));
    value.put(FieldConst.field_es_size_pm, logDetail.get(FieldConst.field_size).getOrElse(0));
    // 取出的时间为秒，将时间转换为整分
    val time = (time_ori / 60) * 60
    value.put(FieldConst.field_es_fromtime, time)
    value.put(FieldConst.field_es_logdate, time * 1000)
    value.put(FieldConst.field_es_timestamp, System.currentTimeMillis())

    // 将时间按分钟取整后放到key中
    sbKey.append(time);
    val key = sbKey.toString();
    //设置主健初始值(id和offset组合)，保存前在主健前拼接批次号，以解决多次录入问题
    value.put(FieldConst.field_id, key)
    value.put(FieldConst.field_es_kafkaoffset, logDetail.get(FieldConst.field_es_kafkaoffset).getOrElse("0"))
    val res = new Tuple2[String, Map[String, Any]](key, value);
    res;
  }

  def transToTuple5m(datastat_1m: Tuple2[String, Map[String, Any]], mergeKey: Array[String]): Tuple2[String, Map[String, Any]] = {
    val srcValue = datastat_1m._2;
    val value = new scala.collection.mutable.HashMap[String, Any];

    //设置汇总类型字段，用于和5分钟统一处理
    val sbKey = new StringBuffer(FieldConst.prefix_agg_5m);
    //FieldConst.keyStrs_xm_stat
    for (key <- mergeKey) {
      value.put(key, srcValue.get(key).getOrElse(0));
      sbKey.append(srcValue.get(key).getOrElse(0));
    }
    val time_ori = srcValue.get(FieldConst.field_es_fromtime).getOrElse(0).toString().toDouble.toLong
    // 取出的时间为秒，将时间转换为整5分
    val time = (time_ori / 300) * 300;
    value.put(FieldConst.field_es_fromtime, time);
    value.put(FieldConst.field_es_logdate, time * 1000);
    value.put(FieldConst.field_es_timestamp, System.currentTimeMillis())
    sbKey.append(time);
    val key: String = sbKey.toString();
    value.put(FieldConst.field_es_size, srcValue.get(FieldConst.field_es_size).getOrElse(0));
    value.put(FieldConst.field_es_size_pm, srcValue.get(FieldConst.field_es_size).getOrElse(0));
    value.put(FieldConst.field_es_req_count, srcValue.get(FieldConst.field_es_req_count).getOrElse(0));

    //设置主健初始值，保存前在主健前拼接批次号，以解决多次录入问题
    value.put(FieldConst.field_id, key)
    value.put(FieldConst.field_es_kafkaoffset, srcValue.get(FieldConst.field_es_kafkaoffset).getOrElse("0"))
    val results = new ArrayList[Tuple2[String, Map[String, Object]]]();
    val res = new Tuple2[String, Map[String, Any]](key, value);
    res
  }

  //  def parseFusionKafkaBytes(event: Array[Byte]): Map[String, Any] = {
  //    val bodyStr = new String(event);
  //    val res = ParseSrcData.split2Map(event);
  //    val mp = JavaConverters.mapAsScalaMapConverter(res).asScala
  //    mp.asInstanceOf[Map[String, Any]]
  //  }
 def parseFusionKafkaBytesForDetail(event: Array[Byte]): Map[String, Any] = {

    val bodyStr = new String(event);
    val res = ParseSrcData.splitFusion2MapForDetail(event);   
    val mp = JavaConverters.mapAsScalaMapConverter(res).asScala
    
    mp.asInstanceOf[Map[String, Any]]
  }
  def parseFusionKafkaBytesForBand(event: Array[Byte], offset:String): Map[String, Any] = {

    val bodyStr = new String(event);
    val res = ParseSrcData.splitFusion2MapForBand(event);
//    val offset=genPartition0Offset(offsetRanges)
    res.put(FieldConst.field_es_kafkaoffset, offset)
//    println("=====offset====="+offset)
    val mp = JavaConverters.mapAsScalaMapConverter(res).asScala
    
    mp.asInstanceOf[Map[String, Any]]
  }
 
 def parseJXKafkaBytesForBandWithOffset(event: Array[Byte], offset:String): Map[String, Any] = {
    val bodyStr = new String(event);
    val res = ParseSrcData.splitJX2MapForBand(event);
//    val offset=genPartition0Offset(offsetRanges)
    res.put(FieldConst.field_es_kafkaoffset, offset)
    val mp = JavaConverters.mapAsScalaMapConverter(res).asScala
    mp.asInstanceOf[Map[String, Any]]
  }

 def parseJXKafkaBytesForDetail(event: Array[Byte]): Map[String, Any] = {
    val bodyStr = new String(event);
    val res = ParseSrcData.splitJX2MapForDetail(event);  
    val mp = JavaConverters.mapAsScalaMapConverter(res).asScala
    mp.asInstanceOf[Map[String, Any]]
  }
 
  def parseRankKafkaBytes(event: Array[Byte]): Map[String, Any] = {
    val bodyStr = new String(event);
    val res = ParseSrcData.splitRankData2Map(event);
    val mp = JavaConverters.mapAsScalaMapConverter(res).asScala
    mp.asInstanceOf[Map[String, Any]]
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
    //    println("req_error" + req_error)
    urldata(pre.id, pre.time_local, pre.time_unix, domain, request, traffic, req_count, downloadnum, req_error, req401, req403, req404, req500, req501,
      req502, req503, req504)
  }
  def parseLogToRankURLData(line: Map[String, Any]): urldata = {

    val res = splitToData(line)
    res
  }
  def getMd5(str: String): String = {

    val md5Key = DigestUtils.md5Hex(str);

    md5Key;

  }
  def parseDouble(s: String): Double = { try { Some(s.toDouble).getOrElse(0.toDouble) } catch { case _: Throwable => 0.toDouble } }

  def splitToData(line: Map[String, Any]): urldata = {
    try {
      val linem = new HashMap[String, Any]

      val stime_unix = line.get(FieldConst.field_time_unix).getOrElse("0").toString()
      val time_unix = stime_unix.toDouble.longValue()
      //按小时取整
      val time_unix_fix = (time_unix / 3600) * 3600
      val time_local = new DateISO(time_unix_fix * 1000)

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
  case class urldata(id: String, time_local: DateISO, time_unix: Long, domain: String, request: String, traffic: Double, req_count: Double,
                     downloadnum: Double, val req_error: Double,
                     req401: Double, req403: Double, req404: Double, req500: Double, req501: Double,
                     req502: Double, req503: Double, req504: Double)

 
  def setAndModifyID(hm: Map[String, Any]): Map[String, Any] = {
    val unique = hm.get(FieldConst.field_es_kafkaoffset)
    val objID = hm.get(FieldConst.field_id)
    if (!objID.isEmpty) {
      val s: StringBuffer = new StringBuffer()
      s.append(unique.getOrElse("0"))
      s.append("##")
      s.append(objID.getOrElse("0"))
      val md5Key = DigestUtils.md5Hex(s.toString());
      hm.put(FieldConst.field_id, md5Key);
      // hm.put("def2", unique)
    }

    hm

  }

//  def setAndModifyID(hm: Map[String, Any]): Map[String, Any] = {
//    val objID = hm.get(FieldConst.field_id)
//    if (!objID.isEmpty) {
//      val s = objID.getOrElse("0").toString()
//      val md5Key = DigestUtils.md5Hex(s);
//      hm.put(FieldConst.field_id, md5Key);
//      // hm.put("def2", unique)
//    }
//
//    hm
//
//  }

   def genPartitionMinOffset(offset: Array[OffsetRange]): String = {
    val s: StringBuffer = new StringBuffer()
    var partitionMin = offset.apply(0).partition.intValue()
    var offsetPartition0 = offset.apply(0).fromOffset.longValue()
    var untiloff= offset.apply(0).untilOffset
     var count= offset.apply(0).count()   
    for (key: OffsetRange <- offset) {
      val partition = key.partition.intValue()     
      if (partition < partitionMin) {
        partitionMin = partition;
        offsetPartition0 = key.fromOffset.longValue()
//        untiloff=key.untilOffset.longValue()
//        count=key.count()
      }
    }
//    println("===============@@@partitionMin@@@===========["+partitionMin+","+offsetPartition0+"->"+untiloff+","+count+"]")
    s.append(partitionMin).append("-").append(offsetPartition0)
    s.toString()
  }
   
   
//  def genPartition0Offset(offset: Array[OffsetRange]): String = {
//    val s: StringBuffer = new StringBuffer()
//    var partitionMin = offset.apply(0).partition.intValue()
//    var offsetPartition0 = offset.apply(0).fromOffset.longValue()
//    var untiloff= offset.apply(0).untilOffset
//     var count= offset.apply(0).count()
//   // println("===============partitionBegin===========["+partitionMin+","+offsetPartition0+"]")
//    for (key: OffsetRange <- offset) {
//      val partition = key.partition.intValue()     
//      if (partition < partitionMin) {
//        partitionMin = partition;
//        offsetPartition0 = key.fromOffset.longValue()
//        untiloff=key.untilOffset.longValue()
//        count=key.count()
//      }
//    }
// //   println("===============partitionMin===========["+partitionMin+","+offsetPartition0+"->"+untiloff+","+count+"]")
//    s.append(partitionMin).append("-").append(offsetPartition0)
//    s.toString()
//  }

  def genPrefix(offset: Array[OffsetRange]): String = {
    val s: StringBuffer = new StringBuffer()
    for (key: OffsetRange <- offset) {
      s.append(key.partition.intValue())
      s.append("-")
      s.append(key.fromOffset.longValue())
      s.append(";")
    }

    s.toString()
  }
}