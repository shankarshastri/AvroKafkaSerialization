import zio._, zio.stream._, zio.duration._
import blocking.Blocking, clock.Clock
import zio.kafka.consumer._, zio.kafka.producer._, zio.kafka.serde._

import org.apache.kafka.clients.consumer.{ KafkaConsumer, ConsumerRecords, ConsumerRecord }

//object ZioAvroProducerAndConsumer extends App {
//
//
//  val consumer = Consumer.make(ConsumerSettings(List("localhost:9092")))
//
//
//
//  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] = {
//
//  }
//}
