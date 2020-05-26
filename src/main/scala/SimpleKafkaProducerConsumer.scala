import java.time.Duration
import java.util._
import java.util.concurrent.Executors

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._

import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object SimpleKafkaProducerConsumer extends App {
  val topicName = "sample-hello-world"

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put("key.serializer", classOf[StringSerializer].getName)
  producerProps.put("value.serializer", classOf[StringSerializer].getName)
  producerProps.put("ack", "all")
  producerProps.put("timeout.ms", "5000")

  val consumerProps = new Properties()
  consumerProps.put("bootstrap.servers", "localhost:9092")
  consumerProps.put("group.id", "consumer-tutorial")
  consumerProps.put("key.deserializer", classOf[StringDeserializer].getName)
  consumerProps.put("value.deserializer", classOf[StringDeserializer].getName)


  implicit val producerExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

  val producer = new KafkaProducer[String, String](producerProps)
  val consumer = new KafkaConsumer[String, String](consumerProps)

  consumer.subscribe(java.util.Collections.singletonList(topicName))

  val consumerExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

  val f = for(i <- 1 to 100) yield {
    val key = UUID.randomUUID()
    val p = Promise[RecordMetadata]()
    val record = new ProducerRecord(topicName, key.toString, s"hello $key")
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if(exception == null) {
          p.success(metadata)
        } else {
          exception.printStackTrace
          p.failure(exception)
        }
      }
    })
    p.future
  }


  val producerFut = Future.sequence(f)

  val consumerFut = Future {
    while(true) {
      val records = consumer.poll(Duration.ofMillis(1000))
      import scala.jdk.CollectionConverters._
      records.asScala.foreach(println(_))
    }
  }(consumerExecutionContext)

  Await.result(producerFut, scala.concurrent.duration.Duration.Inf)
  Await.result(consumerFut, scala.concurrent.duration.Duration.Inf)

  scala.sys.addShutdownHook {
    println("Running shutdown hook")
    producer.flush()
    consumer.close()
  }
}
