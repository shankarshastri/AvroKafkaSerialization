package messages

import com.sksamuel.avro4s.{AvroNamespace, AvroSchema}

object Models {
  @AvroNamespace("")
  sealed trait Message
  @AvroNamespace("")
  case class UserMessage(name: String) extends Message
  @AvroNamespace("")
  case class SystemMessage(name: String, error: Option[String], newField: Option[String] = None) extends Message

  val msgAvroSchema = AvroSchema[Message]
}