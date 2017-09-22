package com.sinobbd.kafka.hbase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import scala.Tuple2;

public class Kafka2Hbase {

	public  void runApp(String[] args) throws InterruptedException {     
    	
    	SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparStreamingOnKafkaReceiver");
    	// SparkConf conf = new SparkConf().setMaster("spark：//Master:7077").setAppName("SparStreamingOnKafkaReceiver");    
    	JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
    	 Map<String,String> kafkaParameters = new HashMap();
         kafkaParameters.put("metadata.broker.list", "183.131.54.162:9092,183.131.54.163:9092,183.131.54.164:9092,183.131.54.165:9092,183.131.54.166:9092");

         Set<String> topics =new HashSet();
          topics.add("firm_logs");
          
          ////
          Configuration hConf = HBaseConfiguration.create();
          String zookeeper = "183.131.54.144:2181,183.131.54.143:2181,183.131.54.142:2181";
          hConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper);
            hConf.set("hbase.zookeeper.quorum", zookeeper);
           hConf.set("hbase.rpc.timeout", "1800000");
           hConf.set("mapreduce.task.timeout", "1800000");
        	 hConf.set("hbase.defaults.for.version.skip", "true");
        	 Configuration   jobConf = new JobConf(hConf);
        	 
        	 final Broadcast<Configuration> broadcastHbaseConfig =
        		        jssc.sparkContext().broadcast(jobConf);
        			      
        ///////			    

         JavaPairInputDStream<String,String> messages = KafkaUtils.createDirectStream(jssc,
                 String.class, String.class,
                 StringDecoder.class, StringDecoder.class,
                 kafkaParameters,
                 topics);     
         
         JavaPairDStream<ImmutableBytesWritable, Put> lines = messages.mapToPair(new PairFunction<Tuple2<String,String>, ImmutableBytesWritable, Put>() {
			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, String> arg0)
					throws Exception {				
	        	 String rowKey = arg0._1;
  			      String  value = arg0._2;
  			   Tuple2 tp= convertToHbasePut(rowKey, value, "test");
  			 return tp;
			}
        	 
		});
         lines.foreachRDD(new VoidFunction<JavaPairRDD<ImmutableBytesWritable,Put>>() {
			
			@Override
			public void call(JavaPairRDD<ImmutableBytesWritable, Put> rdd)
					throws Exception {
				rdd.saveAsNewAPIHadoopFile("",ImmutableBytesWritable.class, Put.class, MultiTableOutputFormat.class, broadcastHbaseConfig.getValue());
				
			}
		});
    
        jssc.start();
        jssc.awaitTermination();

    }
	

	 public Tuple2<ImmutableBytesWritable,Put> convertToHbasePut(String key, String value ,String tableName){
		    String rowKey = key;
		    Long ts=System.currentTimeMillis();
		    Put put = new Put(Bytes.toBytes(rowKey),ts);		   
		    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info1"), Bytes.toBytes(value));
		     
		    Tuple2<ImmutableBytesWritable,Put> tp2=new Tuple2(new ImmutableBytesWritable(Bytes.toBytes(tableName)), put);
		    return tp2;
		  }

}
