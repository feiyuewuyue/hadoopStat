package com.sinobbd.strider.dota;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConversions.*;

public class StreamingToHbase implements Serializable{

	private static final long serialVersionUID = 1L;

	private static KafkaCluster kafkaCluster = null;

    private static HashMap<String, String> kafkaParam = new HashMap<String, String>();

    private static Broadcast<HashMap<String, String>> kafkaParamBroadcast = null;

    private static scala.collection.immutable.Set<String> immutableTopics = null;
    
    public void startJob(String[]args){


        SparkConf sparkConf = new SparkConf().setAppName("java_spark_kafka_hbase");

        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class});
        Set<String> topicSet = new HashSet<String>();
        topicSet.add("zz_test_strong-nginx-access");


        kafkaParam.put("metadata.broker.list", "183.131.54.162:9092,183.131.54.163:9092,183.131.54.164:9092,183.131.54.165:9092,183.131.54.166:9092");
        kafkaParam.put("group.id", "java_spark_kafka_hbase");
       //kafkaParam.put("auto.offset.reset" , "largest");
      	JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));
     //   final Broadcast<JobConf> broadcastHbaseConfig =
  		//        jssc.sparkContext().broadcast(jobConf);
      	 
        
        // transform java Map to scala immutable.map
        scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParam);
        scala.collection.immutable.Map<String, String> scalaKafkaParam =
                testMap.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(Tuple2<String, String> v1) {
                        return v1;
                    }
                });

        // init KafkaCluster
        kafkaCluster = new KafkaCluster(scalaKafkaParam);

        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
       // JavaConverters.
        immutableTopics = mutableTopics.toSet();
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = kafkaCluster.getPartitions(immutableTopics).right().get();

        // kafka direct stream 初始化时使用的offset数据
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap<TopicAndPartition, Long>();

        // 没有保存offset时（该group首次消费时）, 各个partition offset 默认为0
        if (kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).isLeft()) {

            System.out.println(kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).left().get());

            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }

            //更改，
            // 没有消费过
   /*         String reset = kafkaParam.get("auto.offset.reset").toLowerCase();
            val partitionsE = kafkaCluster.getPartitions(topicSet);
            var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
            if (reset == Some("smallest")) {
              leaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
            } else {
              leaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get
            }
            val offsets = leaderOffsets.map {
              case (tp, offset) => (tp, offset.offset)
            }
            log.warn("offsets: " + offsets)
            kc.setConsumerOffsets(groupId, offsets)
          
           */ 
            
            
        }
        // offset已存在, 使用保存的offset
        else {

            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster.getConsumerOffsets("com.nsfocus.bsa.ys.test", topicAndPartitionSet2).right().get();

            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long)consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
            }

        }

       
        kafkaParamBroadcast = jssc.sparkContext().broadcast(kafkaParam);

        // create direct stream
        JavaInputDStream<String> message = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParam,
                consumerOffsetsLong,
                new Function<MessageAndMetadata<String, String>, String>() {
                    public String call(MessageAndMetadata<String, String> v1) throws Exception {
                        return v1.message();
                    }
                }
        );

        // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        JavaDStream<String> javaDStream = message.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd;
            }
        });

     //   JavaHBaseContext hbaseContext = new JavaHBaseContext(sparkConf, jobconf);
        javaDStream.mapToPair(new PairFunction<String,ImmutableBytesWritable,Put>() {

			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(String v1)
					throws Exception {
				// TODO Auto-generated method stub
				String rowKey=UUID.randomUUID().toString();
				  Long ts=System.currentTimeMillis();
				    Put put = new Put(Bytes.toBytes(rowKey),ts);		   
				    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info1"), Bytes.toBytes(v1));
				     
				    Tuple2<ImmutableBytesWritable,Put> tp2=new Tuple2(new ImmutableBytesWritable(Bytes.toBytes("test")), put);
				return tp2;
			}}).foreachRDD(new VoidFunction<JavaPairRDD<ImmutableBytesWritable,Put>>() {

				@Override
				public void call(JavaPairRDD<ImmutableBytesWritable, Put> t)
						throws Exception {
					// TODO Auto-generated method stub

	                if (!t.isEmpty()){ 

	                	  Configuration hConf = HBaseConfiguration.create();
	                      String zookeeper = "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181";
	                      hConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper);
	                        hConf.set("hbase.zookeeper.quorum", zookeeper);
	                       hConf.set("hbase.rpc.timeout", "1800000");
	                       hConf.set("mapreduce.task.timeout", "1800000");
	                    	 hConf.set("hbase.defaults.for.version.skip", "true");
	                    	 JobConf   jobConf = new JobConf(hConf);
	                	t.saveAsNewAPIHadoopFile("",ImmutableBytesWritable.class, Put.class, MultiTableOutputFormat.class, jobConf);
	                	
	                	

	                for (OffsetRange o : offsetRanges.get()) {

	                    // 封装topic.partition 与 offset对应关系 java Map
	                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
	                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
	                    topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

	                    // 转换java map to scala immutable.map
	                    scala.collection.mutable.Map<TopicAndPartition, Object> testMap =
	                            JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);
	                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
	                            testMap.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
	                                public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
	                                    return v1;
	                                }
	                            });

	                    // 更新offset到kafkaCluster
	                    kafkaCluster.setConsumerOffsets(kafkaParamBroadcast.getValue().get("group.id"), scalatopicAndPartitionObjectMap);

	                }
	                }
	            
				}
			});
        

        jssc.start();
        try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    
    }
    
    public static void main(String[] args) {
    	
    	
        	 StreamingToHbase streamingToHbase=new StreamingToHbase();
        	 streamingToHbase.startJob(args);
        	 
        	 
    	
  }



}
