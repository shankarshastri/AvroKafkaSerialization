package kafka.producer

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.{Future, Promise}

object KafkaUtils {
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
}
