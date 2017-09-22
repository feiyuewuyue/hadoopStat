package org.apache.spark.streaming.kafka

object test {
  
  
  
  
     def compareDate(timeStamp:String):(Boolean)={
		val nowTime=System.currentTimeMillis()/1000;
		val logTime=timeStamp.toLong;
		val dataTime=(logTime - 57600)/86400*86400 + 57600;
		println(dataTime)
		
		if(nowTime-dataTime<=30*24*60*60){  //1491580740    1491321540
			return true;
		}
		return false;
	}
  def main(args: Array[String]): Unit = {
      println( compareDate("1499097600")+"")
  }
  
 
  
}