package org.apache.spark.streaming.kafka

import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.RestRepository
import org.apache.commons.logging.LogFactory
import org.apache.spark.Partitioner

class ESShardPartitionner(settings:String)   {/*
  
  //extends Partitioner
   protected val log = LogFactory.getLog(this.getClass())

      protected var _numPartitions = -1 

      override def numPartitions: Int = {   
        val newSettings = new PropertiesSettings().load(settings)
        val repository = new RestRepository(newSettings)
        val targetShards = repository.getWriteTargetPrimaryShards(newSettings.getNodesClientOnly())
        repository.close()
        _numPartitions = targetShards.size()
        _numPartitions
      }

      override def getPartition(key: Any): Int = {
        val shardId = ShardAlg.shard(key.toString(), _numPartitions)
        shardId
      }

*/}


