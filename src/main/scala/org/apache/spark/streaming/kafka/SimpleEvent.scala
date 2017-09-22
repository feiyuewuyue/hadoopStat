package org.apache.spark.streaming.kafka

import com.fasterxml.jackson.annotation.JsonValue
//import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.untyped._
//import com.metamx.tranquility.test.DirectDruidTest.TimeColumn
import com.metamx.tranquility.typeclass.Timestamper
import org.joda.time.DateTime






case class SimpleEvent(ts: DateTime, foo: String, bar: Int, lat: Double, lon: Double)
{
  @JsonValue
  def toMap: Map[String, Any] = Map(
    "ts" -> (ts.getMillis / 1000),
    "foo" -> foo,
    "bar" -> bar,
    "lat" -> lat,
    "lon" -> lon
  )

  def toNestedMap: Map[String, Any] = Map(
    "ts" -> (ts.getMillis / 1000),
    "data" -> Map("foo" -> foo, "bar" -> bar),
    "geo" -> Map("lat" -> lat, "lon" -> lon)
  )

  def toCsv: String = Seq(ts.getMillis / 1000, foo, bar, lat, lon).mkString(",")
}

object SimpleEvent
{
  implicit val simpleEventTimestamper = new Timestamper[SimpleEvent] {
    def timestamp(a: SimpleEvent) = a.ts
  }

  val Columns = Seq("ts", "foo", "bar", "lat", "lon")

  def fromMap(d: Dict): SimpleEvent = {
    SimpleEvent(
      new DateTime(long(d("ts")) * 1000),
      str(d("foo")),
      int(d("bar")),
      double(d("lat")),
      double(d("lon"))
    )
  }
}