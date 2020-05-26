name := "AvroKafkaSerialization"

version := "0.1"

scalaVersion := "2.12.8"

val zioVersion = "1.0.0-RC18-2"

libraryDependencies ++= Seq("org.apache.kafka" % "kafka-clients" % "2.5.0",
  "org.slf4j" % "slf4j-simple" % "1.7.30",
  "com.sksamuel.avro4s" %% "avro4s-core" % "3.0.9",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",

  "com.sksamuel.avro4s" %% "avro4s-kafka" % "3.0.9",


"org.apache.spark" %% "spark-streaming" % "2.4.5",

  "dev.zio" %% "zio-kafka" % "0.8.0",
  "dev.zio" %% "zio-streams" % zioVersion,
  "dev.zio" %% "zio" % zioVersion)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala" % "2.6.7"


