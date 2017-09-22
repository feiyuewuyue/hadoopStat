package org.apache.spark.streaming.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaSinkWrapper(createProducer: () => KafkaProducer[String, Object]) extends Serializable {
  @transient lazy val producer = createProducer()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))

 
  object KafkaSink {
  def apply(config: java.util.Map[String, Object]): KafkaSinkWrapper = {
    val f = () => {
      val producer = new KafkaProducer[String, Object](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSinkWrapper(f)
  }
}
  
}