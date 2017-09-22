package org.apache.spark.streaming.kafka

import com.metamx.tranquility.druid.SpecificDruidDimensions
import org.apache.curator.framework.CuratorFrameworkFactory
import io.druid.granularity.QueryGranularities
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.druid.DruidLocation
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.beam.ClusteredBeamTuning
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import io.druid.query.aggregation.LongSumAggregatorFactory
import com.metamx.tranquility.spark.BeamFactory
import com.metamx.tranquility.beam.Beam
import com.metamx.common.Granularity
import org.joda.time.Period
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import io.druid.data.input.impl.TimestampSpec
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.common.scala.net.curator.DiscoConfig
import com.metamx.common.scala.net.curator.Disco
import com.metamx.tranquility.finagle.FinagleRegistryConfig


class SimpleEventBeamFactory extends BeamFactory[RongheData1]
{
  // Return a singleton, so the same connection is shared across all tasks in the same JVM.
  def makeBeam: Beam[RongheData1] = SimpleEventBeamFactory.BeamInstance
}

object SimpleEventBeamFactory
{
  val BeamInstance: Beam[RongheData1] = {
    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      "183.131.54.181:2181,183.131.54.182:2181,183.131.54.183:2181",
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

  
    val indexService = "druid/overlord" // Your overlord's druid.service, with slashes replaced by colons.
    val discoveryPath = "/druid/discovery"     // Your overlord's druid.discovery.curator.path
    val dataSource = "foo7"
    val dimensions = IndexedSeq("domain")
    val aggregators = Seq(new LongSumAggregatorFactory("body_bytes_sent", "body_bytes_sent"))
    val isRollup = true
      val finagleRegistry={
      val disco=new Disco(curator,new DiscoConfig{def discoAnnounce =None
      def discoPath=discoveryPath  
      }
      
    )
    new FinagleRegistry(FinagleRegistryConfig.builder.finagleEnableFailFast(false).build(),disco)
  }
    
    // Expects simpleEvent.timestamp to return a Joda DateTime object.
    DruidBeams
      .builder((simpleEvent: RongheData1) =>DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z+08:00").parseDateTime(simpleEvent.time_local))
      .curator(curator).timestampSpec(new TimestampSpec("time_local","dd/MMM/yyyy:HH:mm:ss Z+08:00",null))
      .discoveryPath(discoveryPath).finagleRegistry(finagleRegistry)
      .location(DruidLocation.create(indexService, dataSource))
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularities.NONE))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = Granularity.DAY,
          windowPeriod = new Period("PT2280M"),
          partitions = 5,
          replicants = 1
        )
      )
      .buildBeam()
  }
}