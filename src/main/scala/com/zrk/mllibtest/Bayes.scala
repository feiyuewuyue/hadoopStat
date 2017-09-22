package com.zrk.mllibtest

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors

object Bayes {
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("Bayes")
    
    val sc=new SparkContext(conf)
    val data=MLUtils.loadLabeledPoints(sc, "D://tmp/e.txt")
    val model=NaiveBayes.train(data,1.0)
    model.labels.foreach(println)
    model.pi.foreach(println)
    
    val test=Vectors.dense(0,0, 10)
    println(model.predict(test))
    
  }
  
  
}