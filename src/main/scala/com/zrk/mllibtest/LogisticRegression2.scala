package com.zrk.mllibtest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object LogisticRegression2 {
  val conf=new SparkConf().setMaster("local").setAppName("logisticRegresssion2")
  val sc=new SparkContext(conf)
  
  def main(args: Array[String]): Unit = {
    val data=MLUtils.loadLibSVMFile(sc, "D://tmp/u.txt")
    val splits=data.randomSplit(Array(0.6,0.4), seed=11L)
    val parseData=splits(0)
    val parseTtest=splits(1)
    val model=LogisticRegressionWithSGD.train(parseData, 50)
    println(model.weights)
    val predictAndLabels=parseTtest.map{
      case LabeledPoint(label,features)=>
        val prediction=model.predict(features)
        (prediction,label)
      
    }
    
    val metrics=new MulticlassMetrics(predictAndLabels)
    val precision=metrics.precision
    println("Precision="+precision)
    
    //val model=LogisticRegressionWithSGD.train(data, 50)
    
    
    
  }
  
  
}