package com.sky.dap


import org.apache.spark.sql.{types, _}
import org.apache.log4j.LogManager
import org.apache.avro.generic._
import org.apache.avro.io._
import org.apache.spark.sql.types._
import com.databricks.spark.avro.SchemaConverters
import org.apache.spark.sql.expressions.UserDefinedFunction
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.schemaregistry.client._
import org.apache.spark.sql.functions._


/**
  * @author ${user.name}
  */
object App2 {



  def main(args : Array[String]) {

    val log = LogManager.getLogger("SparkStructuredStreaming")


    val spark = SparkSession
      .builder()
      .appName("SparkStructuredStreaming")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    log.info("Go...")


    val avro_decoder = udf((data : Array[Byte]) => {

      //val SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"nome\",\"type\":\"string\"},{\"name\":\"cognome\",\"type\":\"string\"}]}"
      //val schema2: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING) // we use a Parser to read our schema definition and create a Schema object.

      val schemaRegistry: SchemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:19700/", 1)
      val schemaMetadata : io.confluent.kafka.schemaregistry.client.SchemaMetadata = schemaRegistry.getLatestSchemaMetadata("dead.letter.queue-value")
      val schema : org.apache.avro.Schema = schemaRegistry.getById(schemaMetadata.getId)

      //Confluent Avro deserializer
      val deserializer = new KafkaAvroDeserializer(schemaRegistry)
      val result: GenericRecord = deserializer.deserialize("", data, schema).asInstanceOf[GenericRecord] // questo metodo restituisce un Object ma io lo casto a genericRecord

      var a = new scala.collection.mutable.ListBuffer[String]()

      for (i <- 0 to schema.getFields.size() -1 )
      {
        a +=  result.get(i).toString()
      }
      a.toArray


      //Subtable(result.get("message").toString(),result.get("messageType").toString()) //creo una case class che sembra una delle strutture complesse che puo restituire un udf sql
    }
    )

    val df : Dataset[Row] = spark
      .readStream
      .format("kafka")
      //.option("kafka.bootstrap.servers", "localhost:9092")
      .option("kafka.bootstrap.servers", "10.170.14.37:9092")
      //.option("subscribe", "test")
      .option("subscribe", "dead.letter.queue")
      .option("startingOffsets", "earliest")
      .load()
      .select("value")
      .withColumn("decoded_column",avro_decoder(col("value")))
      .select("decoded_column")

    val df1 = df.groupBy(col("decoded_column")).count()

    val query = df1.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()


  }

}
