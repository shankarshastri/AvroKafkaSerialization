import com.sksamuel.avro4s.AvroSchema

object Models {

  sealed trait Message
  case class UserMessage(name: String) extends Message
  case class SystemMessage(name: String, error: Option[String], newField: Option[String] = None,
                           supField: Option[String] = None) extends Message

  val msgAvroSchema = AvroSchema[Message]
}