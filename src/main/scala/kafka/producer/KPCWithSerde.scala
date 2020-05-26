package kafka.producer

import com.sksamuel.avro4s.RecordFormat


object KPCWithSerde extends App {

  sealed trait L
  case class Message(h: String) extends L
  case class Message1(h1: String) extends L

  val l = RecordFormat[L].to(Message("Hello"))
  l

}
