package com.sky.dap



import java.util.{Collections, Properties, UUID}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import scala.util.Random





object App {


  def createConsumerConfig(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", UUID.randomUUID().toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put("client.id", "foo")
    //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    //props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    //props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    props.put("schema.registry.url", "http://localhost:8081")


    props
  }


  def createProducerConfig(): Properties = {
    val producerConfig: Properties = new Properties()

    producerConfig.put("schema.registry.url", "http://localhost:8081")
    producerConfig.put("bootstrap.servers", "localhost:9092")
    producerConfig.put("client.id", "Producer")
    producerConfig.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    producerConfig.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")

    producerConfig
  }



  def main(args: Array[String]): Unit = {
    println("Hello, world!")

    val key = "key1"

    //jsonToAvro(pomSchema,key,pomMessage,producerConfig)
//    writeRawByte("key1",producerConfig)
    //consumer()
    stringConsumer()


  }

  def jsonToAvro (avroSchema: String, key: String, json: String) = {
    //send avro record from json

    val defaultConfig: Properties = createProducerConfig()

    val parser: Schema.Parser = new Schema.Parser()
    val schema: Schema = parser.parse(avroSchema)

    val converter: JsonAvroConverter  = new JsonAvroConverter()
    val avroRecord2 : GenericRecord = converter.convertToGenericDataRecord(json.getBytes(), schema )

    //Per fare in modo di creare automaticamente lo schema nello schema registry devo passare al KafkaProducer un GenericRecord

    /*Serializzazione
     * usando KafkaAvroSerializer nelle proprietà del produttore (chiave e/o valore) se passo un
     * GenericRecord : lui serializza in Avro l'oggetto e lo pubblica (ha tutte le informazioni per la struttura dello schema)
     * String : lui serializza la stringa in Avro e lo pubblica (nello schema registry ci sarà una stringa )
     * Array[Byte]: lui serializza il binario in Avro e lo pubblica (nello schema registry ci sarà bytes)
    */

    val producer2: KafkaProducer[String,GenericRecord]  = new KafkaProducer[String, GenericRecord](defaultConfig)
    val record2 = new ProducerRecord("topic2", key, avroRecord2)
    producer2.send(record2)
    producer2.close()

  }





  def writeRawByte(key: String) =
  {

    val defaultConfig: Properties = createProducerConfig()
    val producer: KafkaProducer[String,Array[Byte]]  = new KafkaProducer[String, Array[Byte]](defaultConfig)


    val arrayByte = Array[Byte](Integer.parseInt("6D",16).toByte,Integer.parseInt("61",16).toByte,Integer.parseInt("72",16).toByte,Integer.parseInt("69",16).toByte,Integer.parseInt("6F",16).toByte)

    val record = new ProducerRecord("topic4", key, arrayByte)
    producer.send(record)
    producer.close()


  }

  def genericRecordToAvro (avroSchema: String, key: String) = {

    //send avro record building GenericRecord manually

    val defaultConfig: Properties = createProducerConfig()

    val parser: Schema.Parser = new Schema.Parser()
    val schema: Schema = parser.parse(avroSchema)


    val producer: KafkaProducer[String,GenericRecord]  = new KafkaProducer[String, GenericRecord](defaultConfig)

    val avroRecord : GenericRecord = new GenericData.Record(schema)
    avroRecord.put("name", "testUser")

    val record = new ProducerRecord("topic1", key, avroRecord)
    producer.send(record)
    producer.close()
  }


  def consumer() = {

    val props: Properties = createConsumerConfig()
    val consumer = new KafkaConsumer[String, Array[Byte]](props)
    consumer.subscribe(Collections.singletonList("topic4"))

    while (true) {
      val records : ConsumerRecords[String,Array[Byte]] = consumer.poll(1000)
      consumer.commitSync()
    for (record <- records.asScala) {
      val a : Array[Byte] = record.value()
      println("Received message: (" + record.key() + ", " + record.value().toString + ") at offset " + record.offset())
      println(a.toString)
    }
  }
  }


  def stringConsumer() = {

    val props: Properties = createConsumerConfig()
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("topic4"))

    while (true) {
      val records : ConsumerRecords[String,String] = consumer.poll(1000)
      consumer.commitSync()
      for (record <- records.asScala) {
        val a : String = record.value()
        println("Received message: (" + record.key() + ", " + record.value().toString + ") at offset " + record.offset())
        println(a.toString)
      }
    }
  }
}
