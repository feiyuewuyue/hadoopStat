package com.sinobbd.strider.dota.band

import com.sinobbd.data.merge.ParseSrcData
import scala.collection.mutable.Map
import scala.collection.JavaConverters

object BusinessProcess {
  def parseJXLog(userlog: String, otherlog: String): Map[String, Any] = {
    val res = ParseSrcData.splitJXHbase2Map(userlog, otherlog);
    val mp = JavaConverters.mapAsScalaMapConverter(res).asScala
    mp.asInstanceOf[Map[String, Any]]
  }

  def parseFusionLog(userlog: String, otherlog: String): Map[String, Any] = {
    val res = ParseSrcData.splitFusionHbase2Map(userlog, otherlog);
    val mp = JavaConverters.mapAsScalaMapConverter(res).asScala
    mp.asInstanceOf[Map[String, Any]]
  }

}