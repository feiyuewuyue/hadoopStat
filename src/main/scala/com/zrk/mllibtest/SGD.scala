package com.zrk.mllibtest

import scala.collection.mutable.HashMap

object SGD {
  val data=HashMap[Int,Int]()
  def getData():HashMap[Int,Int]={
    for(i<-1 to 50){
      data+=(i->2*i)
    }
    data
  }
  
  
  
  var theot:Double=0
  var arf:Double=0.1
  def sgd(x:Double,y:Double)={
    theot=theot-arf*((theot*x)-y)
  }
  
  def main(args:Array[String]){
    val dataSource=getData()
    dataSource.foreach(myMap=>{
      sgd(myMap._1,myMap._2)
      
    })
    println("theot="+theot)
  }
  
}