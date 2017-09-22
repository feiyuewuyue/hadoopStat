package com.zrk.mllibtest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.Map

object CosCollaborativeFilteringSpark {
  val conf=new SparkConf().setMaster("local").setAppName("CollarativeFilter")
  val sc=new SparkContext(conf)
  
  val users=sc.parallelize(Array("aaa","bbb","ccc","ddd","eee"))
  val films=sc.parallelize(Array("smzdm","ylxb","znh","nhsc","fcwr"))
  val source=Map[String,Map[String,Int]]()
  val fileSource=Map[String,Int]()
  
  def getSource():Map[String,Map[String,Int]]={
    val user1FileSource=Map("smzdm"->2,"ylxb"->3,"znh"->1,"nhsc"->0,"fcwr"->1)
    val user2FileSource=Map("smzdm"->1,"ylxb"->2,"znh"->2,"nhsc"->1,"fcwr"->4)
    val user3FileSource=Map("smzdm"->2,"ylxb"->1,"znh"->0,"nhsc"->1,"fcwr"->4)
    val user4FileSource=Map("smzdm"->3,"ylxb"->2,"znh"->0,"nhsc"->5,"fcwr"->3)
    val user5FileSource=Map("smzdm"->5,"ylxb"->3,"znh"->1,"nhsc"->1,"fcwr"->2)
    source += ("aaa"->user1FileSource)
    source+=("bbb"->user2FileSource)
    source+=("ccc"->user3FileSource)
    source+=("ddd"->user4FileSource)
    source+=("eee"->user5FileSource)
    
   source
  }
  
  def getCollaborateSource(user1:String,user2:String):Double={
    val user1FileSource=source.get(user1).get.values.toVector
    val user2FileSource=source.get(user2).get.values.toVector
    println(user2+user1FileSource.zip(user2FileSource))
    val member=user1FileSource.zip(user2FileSource).map(d=>d._1*d._2).reduce(_ + _).toDouble
  
    
    val temp1=math.sqrt(user1FileSource.map(num=>{math.pow(num,2)}).reduce(_+_))
    val temp2=math.sqrt(user2FileSource.map(num=>{math.pow(num, 2)}).reduce(_+_))
    val denominator=temp1*temp2
    member/denominator
  }
  def main(args:Array[String]){
    getSource()
    val name="bbb"
    users.foreach(user=>{
     println(name+"相对于"+user+"相似度"+getCollaborateSource(name, user)) 
    })
    
    
  }
  
}