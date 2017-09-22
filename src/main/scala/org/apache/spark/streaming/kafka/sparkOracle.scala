package org.apache.spark.streaming.kafka

import java.sql.DriverManager
 
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object sparkOracle {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark oracle").setMaster("local[2]")
    val sc = new SparkContext(conf)
 
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
        DriverManager.getConnection("jdbc:oracle:thin:@//192.168.51.72:1521/XE ", "INFA_DOM", "zrk1234567")
      },
      "SELECT * FROM users",
      1, 10, 1,
      r => (r.getString(1),r.getString(2),r.getString(3)))
    rdd.collect().foreach(println)
    sc.stop()
  }
}