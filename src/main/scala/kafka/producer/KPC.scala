package kafka.producer

import java.io.ByteArrayOutputStream
import java.util.{Properties, UUID}
import java.util.concurrent.Executors

import KafkaUtils._
import com.sksamuel.avro4s.AvroOutputStream
import messages.Models._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object KPC extends App {
  val topicName = "sample-hello-world-avro"

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
  producerProps.put("acks", "all")
  producerProps.put("timeout.ms", "5000")
  producerProps.put("batch.size", "10")
  producerProps.put("linger.ms", "1000")
  producerProps.put("retries", "5")
  producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")

  implicit val producerExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

  val producer = new KafkaProducer[String, Array[Byte]](producerProps)


  val f = for(i <- 1 to 100) yield {
    val key = UUID.randomUUID()
    val p = Promise[RecordMetadata]()

    val data = if(scala.util.Random.nextInt(1000) % 2 == 0) UserMessage(s"Hello ${key}") else SystemMessage(s"Hello System Message ${key}", None, Some("Super"))
    val b = new ByteArrayOutputStream()
    val k = AvroOutputStream.json[Message].to(b).build(msgAvroSchema)
    k.write(data)
    k.flush()

    producer.sendAsync(new ProducerRecord[String, Array[Byte]](topicName, key.toString, b.toByteArray))
  }


  val producerFut = Future.sequence(f)

  Await.result(producerFut, scala.concurrent.duration.Duration.Inf)
  producerFut.onComplete(e => println(e))

  scala.sys.addShutdownHook {
    println("Running shutdown hook")
    producer.flush()
  }
}
