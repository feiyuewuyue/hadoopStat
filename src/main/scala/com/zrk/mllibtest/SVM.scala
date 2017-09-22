package com.zrk.mllibtest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.SVMWithSGD

object SVM {
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("SVM")
    val sc=new SparkContext(conf)
    val data=sc.textFile("D://tmp/u.txt")
    val parsedData=data.map{
      line=> 
        val parts=line.split("|")
        LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()
    
    val model=SVMWithSGD.train(parsedData, 10)
    println(model.intercept)
    println(model.weights)
    
    
    
    
    
  }
}