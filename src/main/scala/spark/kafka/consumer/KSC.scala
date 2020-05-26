package spark.kafka.consumer

import com.sksamuel.avro4s.AvroInputStream
import messages.Models
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.util.Try

object KSC extends App {

  import org.apache.kafka.common.serialization.StringDeserializer
  import org.apache.spark.streaming.kafka010._
  import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

  import org.apache.spark._
  import org.apache.spark.streaming._


  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val streamingContext = new StreamingContext(conf, Minutes(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[ByteArrayDeserializer],
    "group.id" -> "consumer",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("sample-hello-world-avro")
  val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
    streamingContext,
    PreferConsistent,
    Subscribe[String, Array[Byte]](topics, kafkaParams)
  )

  stream.map {
    record => {
      (record.key, record.value().mkString, Try(AvroInputStream.json[Models.Message].from(record.value()).build(Models.msgAvroSchema).tryIterator.toList))
    }
  }.print()

  streamingContext.start()
  streamingContext.awaitTermination()
}
