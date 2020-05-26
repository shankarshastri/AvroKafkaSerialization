import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util._
import java.util.concurrent.Executors

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.producer.ProducerConfig
import Models._
import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Try

object SimpleAvroKafkaProducerConsumer extends App {

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
            case ex: Exception => exceptionHandler(ex)
          }
        }
      }
      timer.schedule(timerTask, pollLongMillis, pollLongMillis)
    }
  }



  val topicName = "sample-hello-world-avro"

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
  producerProps.put("ack", "all")
  producerProps.put("timeout.ms", "5000")
  producerProps.put("batch.size", "10")
  producerProps.put("linger.ms", "1000")
  producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")

  val consumerProps = new Properties()
  consumerProps.put("bootstrap.servers", "localhost:9092")
  consumerProps.put("group.id", "consumer-tutorial")
  consumerProps.put("key.deserializer", classOf[StringDeserializer].getName)
  consumerProps.put("value.deserializer", classOf[ByteArrayDeserializer].getName)



  implicit val producerExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

  val producer = new KafkaProducer[String, Array[Byte]](producerProps)
  val consumer = new KafkaConsumer[String, Array[Byte]](consumerProps)

  consumer.subscribe(java.util.Collections.singletonList(topicName))

  val consumerExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

  val f = for(i <- 1 to 100) yield {
    val key = UUID.randomUUID()
    val p = Promise[RecordMetadata]()

    val data = if(scala.util.Random.nextInt(100) % 2 == 0) UserMessage(s"Hello ${key}") else SystemMessage(s"Hello System Message ${key}", None, Some("Super"), Some("ResultValue"))
    val b = new ByteArrayOutputStream()
    val k = AvroOutputStream.json[Message].to(b).build(msgAvroSchema)
    k.write(data)
    k.flush()


    val record = new ProducerRecord[String, Array[Byte]](topicName, key.toString, b.toByteArray)
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
      println(records.count())
      records.asScala.foreach {
        e => {
          println(s"key: ${e.key()}")
          println(s"value: ${Try(AvroInputStream.json[Message].from(e.value()).build(msgAvroSchema).tryIterator.toList)}")
        }
      }
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
