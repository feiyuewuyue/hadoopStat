package org.apache.spark.streaming.kafka

import java.io.InputStreamReader


import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import com.google.common.io.CharStreams
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule


/**  * 通过http请求的方式,可以
  * 1. 向druid里面发送数据
  * 2. 提供一些查询的druid的方法
  * 3. 顺带查询hbase数据的方法
  */
object Https {
  private val httpClient = HttpClients.createDefault()

  def get(url: String): String = {
    val _get = new HttpGet(url)
    val resp = httpClient.execute(_get)
    try {
      if (resp.getStatusLine.getStatusCode != 200) {
        throw new RuntimeException("error: " + resp.getStatusLine)
      }
      CharStreams.toString(new InputStreamReader(resp.getEntity.getContent))
    } finally {
      resp.close()
    }
  }

//既可以发送数据，也可以请求数据(以结果的形式返回)
  def post(url: String, content: String): String = {
    val _post = new HttpPost(url)
    _post.setHeader("Content-Type", "application/json")
    _post.setEntity(new StringEntity(content,"utf-8"))
    val resp = httpClient.execute(_post)
    try {
      CharStreams.toString(new InputStreamReader(resp.getEntity.getContent))
    } finally {
      resp.close()
    }

  }

  object MapTypeRef extends com.fasterxml.jackson.core.`type`.TypeReference[Map[String, Any]]

  object ListMapTypeRef extends com.fasterxml.jackson.core.`type`.TypeReference[List[Map[String, Any]]]

  def queryHBase(sql: String): List[Map[String, Any]] = {
//将request为json格式
    val request  = new String(Mapper.mapper.writeValueAsBytes(Map(
      "action" -> "query",
      "sql" -> sql
    )))
    //发送json格式的请求
    val resp = post("http://reynold-master:8209/api", request)
    val rs = Mapper.mapper.readValue[Map[String, Any]](resp, MapTypeRef)
    //val rs = Mapper.mapper.readValue[Map[String, Any]](resp, classOf[Map[String, Any]]) 这种方式也可以
    rs("result").asInstanceOf[List[Map[String, Any]]]
  }

  def queryDruid(json: String): String = {
    post("http://reynold-master:18082/druid/v2", json)
  }

  private def getDruid(path: String): String = {
    get("http://reynold-master:18082/druid/v2" + path)
  }

  def druidDataSources(): List[String] = {
    Mapper.mapper.readValue[List[String]](getDruid("/datasources"), ListMapTypeRef)
  }

  def druidDimension(datasource: String): String = {
    getDruid(s"/datasources/$datasource/dimensions")
  }

  def druidMetrics(datasource: String): String = {
    getDruid(s"/datasources/$datasource/metrics")
  }

  def main(args: Array[String]): Unit = {
    println(queryHBase("select * from user_tags limit 2"))
  }

}




/**
  * Created by reynold
  */

object Mapper {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
}