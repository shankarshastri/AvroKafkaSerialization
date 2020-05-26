package spark.kafka.consumer

import java.nio.charset.Charset

import com.sksamuel.avro4s.AvroInputStream
import messages.Models
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.util.{Success, Try}


object KSSC extends App {
  import org.apache.spark.sql._


  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._
  val topics = Array("sample-hello-world-avro")

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("group.id", "consumer-structured-streaming")
    .option("subscribe", "sample-hello-world-avro")
    .load()

  val query = df.selectExpr("CAST(key AS STRING)", "value")
    .as[(String, Array[Byte])].map(e => (e._1, e._2.mkString,
    AvroInputStream.json[Models.Message].from(e._2).build(Models.msgAvroSchema).tryIterator.toList.toString()))
    .writeStream.format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()


  query.awaitTermination()
}
