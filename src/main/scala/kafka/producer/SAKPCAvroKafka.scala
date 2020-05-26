package kafka.producer

import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.concurrent.Executors
import java.util.{Properties, Timer, TimerTask, UUID}


import com.sksamuel.avro4s._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization._
import com.sksamuel.avro4s.BinaryFormat


import com.sksamuel.avro4s.kafka.GenericSerde
import scala.concurrent._
import scala.util.Try

object SAKPCAvroKafka extends App {
  implicit class KafkaUtil[K,V](kP: KafkaProducer[K,V]) {
    def sendAsync(record: ProducerRecord[K,V]): Future[RecordMetadata] = {
      val p = Promise[RecordMetadata]()
      kP.send(record, new Callback {
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
  }

  implicit class KafkaUtilForConsumer[K,V](kC: KafkaConsumer[K,V]) {
    import scala.jdk.CollectionConverters._
    private val timer = new Timer()
    def scheduledPoll(pollLongMillis: Long, callback: Iterable[ConsumerRecord[K,V]] => Try[Unit],
                      exceptionHandler: Exception => Try[Unit])
                     (implicit ec: ExecutionContext): Unit = {
      val timerTask = new TimerTask {
        override def run(): Unit = {
          Try {
            val records = kC.poll(Duration.ofMillis(pollLongMillis))
            callback(records.asScala)
          }.flatten.recoverWith {
            case ex: WakeupException => Try(kC.close())
            case ex: Exception => exceptionHandler(ex)
          }
        }
      }
      timer.schedule(timerTask, pollLongMillis, pollLongMillis)
    }
  }


  val topicName = "sample-hello-world-avro-local-serde"

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new GenericSerde[KafkaValue](BinaryFormat).getClass.getCanonicalName)
  producerProps.put("ack", "all")
  producerProps.put("timeout.ms", "5000")
  producerProps.put("batch.size", "10")
  producerProps.put("linger.ms", "1000")
  producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")

  val consumerProps = new Properties()
  consumerProps.put("bootstrap.servers", "localhost:9092")
  consumerProps.put("group.id", "consumer-tutorial")
  consumerProps.put("key.deserializer", classOf[StringDeserializer].getName)
  consumerProps.put("value.deserializer", new GenericSerde[KafkaValue](BinaryFormat))

  sealed trait KafkaValue
  case class KafkaValue1(name: String, location: String) extends KafkaValue
  case class KafkaValue2(name1: String, location1: String) extends KafkaValue


  implicit val producerExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

  val producer = new KafkaProducer[String, KafkaValue](producerProps)
  val consumer = new KafkaConsumer[String, KafkaValue](consumerProps)

  consumer.subscribe(java.util.Collections.singletonList(topicName))

  val consumerExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

  val f = for(i <- 1 to 100) yield {
    val key = UUID.randomUUID()
    val p = Promise[RecordMetadata]()

    val data = if(scala.util.Random.nextInt(100) % 2 == 0) KafkaValue1("Nam","location") else KafkaValue1("Nam1","location1")

    producer.sendAsync(new ProducerRecord[String, KafkaValue](topicName, key.toString, data))
  }


  val producerFut = Future.sequence(f)


  val consumeFut = consumer.scheduledPoll(10000, (it) => {
    Try {
      it.foreach { e => {
        println(s"key: ${e.key()}")
        println(s"valueInValue ${e.value()}")
      }
      }
    }
  }, (ex) => {
    Try(())
  })

  Await.result(producerFut, scala.concurrent.duration.Duration.Inf)

  scala.sys.addShutdownHook {
    println("Running shutdown hook")
    producer.flush()
    consumer.wakeup()
  }

}

