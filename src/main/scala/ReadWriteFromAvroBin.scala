//import java.io.ByteArrayOutputStream
//
//import com.sksamuel.avro4s._
//import org.apache.avro.Schema
//
//import scala.util.Try
//
//object ReadWriteFromAvroBin {
//  def write[T >: Null : SchemaFor : Encoder : Decoder](msg: T, schema: Schema): Array[Byte] = {
//    val b = new ByteArrayOutputStream()
//    val k = AvroOutputStream.binary[T].to(b).build(schema)
//    k.write(msg)
//    k.flush()
//    b.toByteArray
//  }
//
//  def read[T >: Null : SchemaFor : Encoder : Decoder](msg: Array[Byte], schema: Schema): List[Try[T]] = {
//    val s = AvroInputStream.binary[T].from(msg).build(schema)
//    s.tryIterator.toList
//  }
//}
