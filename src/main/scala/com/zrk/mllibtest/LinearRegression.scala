package com.zrk.mllibtest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object LinearRegression {
  
  val conf=new SparkConf().setMaster("local").setAppName("LinearRegression")
  val sc=new SparkContext(conf)
  
  def main(args:Array[String])={
    val data=sc.textFile("D://tmp/b.txt")
    val parsedData=data.map{line=>
      val parts=line.split(",")
      println(Vectors.dense(parts(1).split(" ").map(_.toDouble)))
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()
    //建立模型
    val model=LinearRegressionWithSGD.train(parsedData, 100,0.1)
   parsedData.foreach(println)
   val result=model.weights
   val result1=model.predict(Vectors.dense(2, 2))
  println(""+result+"   "+result1)
    
    
  }
  
}