package com.sky.dap


import org.apache.spark.sql._
import org.apache.log4j.LogManager
import org.apache.avro.generic._
import org.apache.avro.io._


/**
 * @author ${user.name}
 */
object App {


   // https://github.com/Tubular/confluent-spark-avro/blob/master/src/main/scala/com/databricks/spark/avro/ConfluentSparkAvroUtils.scala
  def decode(data : Array[Byte]) : String =
  {
    val SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"nome\",\"type\":\"string\"}]}"
    val schema: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING) // we use a Parser to read our schema definition and create a Schema object.

    val magicByte: Byte = data(0)
    val payload : Array[Byte] = data.slice(5,data.length)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get.binaryDecoder(payload, null)
    return reader.read(null, decoder).get(0).toString()
  }


  def dead_letter_avro_decoder(data : Array[Byte]) : String =
  {
    val SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"DeadLetterEvent\",\"namespace\":\"com.sky.dap.model\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"},{\"name\":\"messageType\",\"type\":\"string\"},{\"name\":\"sourceName\",\"type\":\"string\"},{\"name\":\"sourceType\",\"type\":\"string\"},{\"name\":\"target\",\"type\":\"string\"},{\"name\":\"targetType\",\"type\":\"string\"},{\"name\":\"processName\",\"type\":\"string\"},{\"name\":\"processType\",\"type\":\"string\"},{\"name\":\"errors\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}"
    val schema: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING) // we use a Parser to read our schema definition and create a Schema object.

    val magicByte: Byte = data(0)
    val payload : Array[Byte] = data.slice(5,data.length)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get.binaryDecoder(payload, null)
    return reader.read(null, decoder).get("messageType").toString()

  }



    def main(args : Array[String]) {

      val log = LogManager.getLogger("SparkStructuredStreaming")


      val spark = SparkSession
        .builder()
        .appName("SparkStructuredStreaming")
          .master("local[2]")
        .getOrCreate()

      import spark.implicits._

      log.info("Go...")

      val df : Dataset[Row] = spark
        .read
        .format("kafka")
        //.option("kafka.bootstrap.servers", "10.170.14.18:9092")
        .option("kafka.bootstrap.servers", "10.170.14.37:9092")
        //.option("subscribe", "gb.nowtv.ac.dcm.stream_stop")
        //.option("subscribe", "dead.letter.queue")
        .option("subscribe", "dead.letter.queue")
        .option("startingOffsets", "earliest")
        .load()


      spark.udf.register("deserialize", dead_letter_avro_decoder _)


      val df1 : Dataset[Row] = df
        //.select("value").as(org.apache.spark.sql.Encoders.BINARY)
        .selectExpr("deserialize(CAST(value AS STRING)) as rows", "value")

      df1.show(100,false)


      //println("spurio" + decode_horton(df.rdd.first().get(1).asInstanceOf[Array[Byte]]))

      //val mystream = Array[Byte](0.toByte, 0.toByte, 0.toByte, 0.toByte, 1.toByte, 6.toByte, 66.toByte, 6F.toByte, 6F.toByte)
      //val mystream = Array[Byte](6.toByte, 66.toByte, 6F.toByte, 6F.toByte)

      //println("my stream to string " + mystream.toString())
      //println("decoded " + decode_tabular(mystream))


      log.info("Bye...")

      spark.stop()
  }

}
