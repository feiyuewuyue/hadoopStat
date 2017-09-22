package com.zrk.mllibtest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

object ALSCollaborativeFilter {
  
  
  def main(args:Array[String]){
    val conf= new SparkConf().setMaster("local").setAppName("ALSCollaborative")
    val sc=new SparkContext(conf)
    val data=sc.textFile("D://tmp/a.txt")
    val ratings=data.map(_.split(' ')  match {
      case Array(user,item,rate) =>
        Rating(user.toInt,item.toInt,rate.toDouble)
    })
    val rank=2
    val numIterations=2
    val model=ALS.train(ratings, rank, numIterations,0.02)
    var rs=model.recommendProducts(2, 1)
    rs.foreach(println)
    
  }
}