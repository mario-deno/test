package com.sky.dap


import org.apache.spark.sql._
import org.apache.log4j.LogManager
import org.apache.avro.generic._
import org.apache.avro.io._
import org.apache.spark.sql.types._
import com.databricks.spark.avro.SchemaConverters
import org.apache.spark.sql.expressions.UserDefinedFunction
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.schemaregistry.client._
import org.apache.spark.sql.api.java.UDF1






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



  def decode_confluent(data : Array[Byte]) : String =
  {

    val SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"nome\",\"type\":\"string\"},{\"name\":\"cognome\",\"type\":\"string\"}]}"
    val schema: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING) // we use a Parser to read our schema definition and create a Schema object.


    val schemaRegistry : SchemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8081/",1)

    //Confluent Avro deserializer
    val deserializer = new KafkaAvroDeserializer(schemaRegistry)
    //val result : Object = deserializer.deserialize("",data,schema) // questo restituisce un Object e devo cercare di ritornarlo in forma di colonna..
    val result2 : GenericRecord =  deserializer.deserialize("",data,schema).asInstanceOf[GenericRecord] // questo metodo restituisce un Object ma io lo casto a genericRecord
    //val result3 : IndexedRecord =  deserializer.deserialize("",data,schema).asInstanceOf[IndexedRecord] // questo metodo restituisce un object ma io lo casto a genericRecord




    return result2.get("nome").toString()

  }



  def decode_confluent_struct(data : Array[Byte]) : StructType =
  {
    val SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"nome\",\"type\":\"string\"},{\"name\":\"cognome\",\"type\":\"string\"}]}"
    val schema: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING) // we use a Parser to read our schema definition and create a Schema object.

    val schemaRegistry : SchemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8081/",1)

    //Confluent Avro deserializer
    val deserializer = new KafkaAvroDeserializer(schemaRegistry)
    //val result : Object = deserializer.deserialize("",data,schema) // questo restituisce un Object e devo cercare di ritornarlo in forma di colonna..
    val result2 : GenericRecord =  deserializer.deserialize("",data,schema).asInstanceOf[GenericRecord] // questo metodo restituisce un Object ma io lo casto a genericRecord
    //val result3 : IndexedRecord =  deserializer.deserialize("",data,schema).asInstanceOf[IndexedRecord] // questo metodo restituisce un object ma io lo casto a genericRecord

    return RowFactory.create(result2.get("nome").toString(), result2.get("cognome").toString()).schema //sql.Row

  }


  def dead_letter_avro_decoder(data : Array[Byte]) : String =
  {
    val SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"DeadLetterEvent\",\"namespace\":\"com.sky.dap.model\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"},{\"name\":\"messageType\",\"type\":\"string\"},{\"name\":\"sourceName\",\"type\":\"string\"},{\"name\":\"sourceType\",\"type\":\"string\"},{\"name\":\"target\",\"type\":\"string\"},{\"name\":\"targetType\",\"type\":\"string\"},{\"name\":\"processName\",\"type\":\"string\"},{\"name\":\"processType\",\"type\":\"string\"},{\"name\":\"errors\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}"
    val schema: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING) // we use a Parser to read our schema definition and create a Schema object.

    val magicByte: Byte = data(0)
    val payload : Array[Byte] = data.slice(5,data.length)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder : org.apache.avro.io.Decoder  = DecoderFactory.get.binaryDecoder(payload, null)

    return reader.read(null, decoder).get("messageType").toString()

  }


   //https://github.com/Tubular/confluent-spark-avro/blob/master/src/main/scala/com/databricks/spark/avro/ConfluentSparkAvroUtils.scala
  def dead_letter_avro_decoder_confluent(data : Array[Byte]) : String =
  {
    val SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"DeadLetterEvent\",\"namespace\":\"com.sky.dap.model\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"},{\"name\":\"messageType\",\"type\":\"string\"},{\"name\":\"sourceName\",\"type\":\"string\"},{\"name\":\"sourceType\",\"type\":\"string\"},{\"name\":\"target\",\"type\":\"string\"},{\"name\":\"targetType\",\"type\":\"string\"},{\"name\":\"processName\",\"type\":\"string\"},{\"name\":\"processType\",\"type\":\"string\"},{\"name\":\"errors\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}"
    val schema: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING) // we use a Parser to read our schema definition and create a Schema object.



    //val struct : StructType = StructType(StructField("nome",DataTypes.IntegerType,false)::StructField("cognome",DataTypes.IntegerType,false) ::Nil) //crea un tipo strutturato
    //val e =  struct.fields
    //val a : DataType = DataTypes.IntegerType


    //val avroRecord : GenericRecord = new GenericData.Record(schema);

    //Confluent Avro deserializer
    val deserializer = new KafkaAvroDeserializer()
    //val result : Object = deserializer.deserialize("",data,schema) // questo restituisce un Object e devo cercare di ritornarlo in forma di colonna..
    val result2 : GenericRecord =  deserializer.deserialize("",data,schema).asInstanceOf[GenericRecord] // questo metodo restituisce un object ma io lo casto a genericRecord
    //val result3 : IndexedRecord =  deserializer.deserialize("",data,schema).asInstanceOf[IndexedRecord] // questo metodo restituisce un object ma io lo casto a genericRecord



    //val datatype : StructType  = SchemaConverters.toSqlType(schema).asInstanceOf[StructType] //This object contains method that are used to convert sparkSQL schemas to avro schemas and viceversa
    //return org.apache.spark.sql.types.DataTypes.createStructType(datatype.fields)


    //DataTypes

    return result2.get("message").toString()


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
        .option("kafka.bootstrap.servers", "localhost:9092")
        //.option("kafka.bootstrap.servers", "10.170.14.37:9092")
        .option("subscribe", "test1")
        //.option("subscribe", "dead.letter.queue")
        .option("startingOffsets", "earliest")
        .load()


      val SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"nome\",\"type\":\"string\"},{\"name\":\"cognome\",\"type\":\"string\"}]}"
      val schema: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING) // we use a Parser to read our schema definition and create a Schema object.
      val typest = com.databricks.spark.avro.SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]; //da tipi di schema avro a tipi spark sql






      val df1 : Dataset[Row] = df
        //.selectExpr("deserialize(CAST(value AS STRING)) as rows")
        .selectExpr("deserialize(value) as rows")
        //.selectExpr("deserialize(CAST(value AS STRING)) as rows")
        .select("rows.*")

      df1.show(100,false)


      //println("spurio" + decode_horton(df.rdd.first().get(1).asInstanceOf[Array[Byte]]))

      //val mystream = Array[Byte](0.toByte, 0.toByte, 0.toByte, 0.toByte, 1.toByte, 6.toByte, 66.toByte, 6F.toByte, 6F.toByte)
      //val mystream = Array[Byte](6.toByte, 66.toByte, 6F.toByte, 6F.toByte)

      //val mystream = Array[Byte](0.toByte,0.toByte,0.toByte,0.toByte,0D.toByte,4.toByte, 7B.toByte, 7D.toByte, 2A.toByte, 4B.toByte,41.toByte,46.toByte,4B.toByte,41.toByte,5F.toByte,4A.toByte,53.toByte,4F.toByte,4E.toByte,5F.toByte,56.toByte,41.toByte,4C.toByte,49.toByte,44.toByte,41.toByte,54.toByte,49.toByte,4F.toByte,4E.toByte,34.toByte,67.toByte,62.toByte,2E.toByte,6E.toByte,6F.toByte,77.toByte,74.toByte,76.toByte,2E.toByte,61.toByte,63.toByte,2E.toByte,76.toByte,69.toByte,64.toByte,65.toByte,6F.toByte,2E.toByte,70.toByte,6C.toByte,61.toByte,79.toByte,5F.toByte,76.toByte,6F.toByte,64.toByte,0A.toByte,4B.toByte,41.toByte,46.toByte,4B.toByte,41.toByte,04.toByte,4E.toByte,41.toByte,0A.toByte,4B.toByte,41.toByte,46.toByte,4B.toByte,41.toByte,24.toByte,65.toByte,75.toByte,2D.toByte,70.toByte,6F.toByte,72.toByte,74.toByte,61.toByte,62.toByte,69.toByte,6C.toByte,69.toByte,74.toByte,79.toByte,2E.toByte,73.toByte,6A.toByte,31.toByte,26.toByte,53.toByte,50.toByte,41.toByte,52.toByte,4B.toByte,5F.toByte,4A.toByte,4F.toByte,42.toByte,5F.toByte,53.toByte,54.toByte,52.toByte,45.toByte,41.toByte,4D.toByte,49.toByte,4E.toByte,47.toByte,BE.toByte,01.toByte,4E.toByte,6F.toByte,20.toByte,75.toByte,73.toByte,61.toByte,62.toByte,6C.toByte,65.toByte,20.toByte,76.toByte,61.toByte,6C.toByte,75.toByte,65.toByte,20.toByte,66.toByte,6F.toByte,72.toByte,20.toByte,70.toByte,72.toByte,6F.toByte,70.toByte,6F.toByte,73.toByte,69.toByte,74.toByte,69.toByte,6F.toByte,6E.toByte,0A.toByte,44.toByte,69.toByte,64.toByte,20.toByte,6E.toByte,6F.toByte,74.toByte,20.toByte,66.toByte,69.toByte,6E.toByte,64.toByte,20.toByte,76.toByte,61.toByte,6C.toByte,75.toByte,65.toByte,20.toByte,77.toByte,68.toByte,69.toByte,63.toByte,68.toByte,20.toByte,63.toByte,61.toByte,6E.toByte,20.toByte,62.toByte,65.toByte,20.toByte,63.toByte,6F.toByte,6E.toByte,76.toByte,65.toByte,72.toByte,74.toByte,65.toByte,64.toByte,20.toByte,69.toByte,6E.toByte,74.toByte,6F.toByte,20.toByte,6A.toByte,61.toByte,76.toByte,61.toByte,2E.toByte,6C.toByte,61.toByte,6E.toByte,67.toByte,2E.toByte,53.toByte,74.toByte,72.toByte,69.toByte,6E.toByte,67.toByte,9C.toByte,A3.toByte,C5.toByte,8E.toByte,B7.toByte,58.toByte)


      //println("my stream to string " + mystream.toString())
      //println("decoded " + decode_tabular(mystream))


      log.info("Bye...")

      spark.stop()
  }

}
