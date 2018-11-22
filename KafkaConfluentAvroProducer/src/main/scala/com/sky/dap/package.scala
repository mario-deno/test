package com.sky.dap

import java.util.Properties
import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.file.{Files, Paths}


/**
  * Created by dap on 7/3/18.
  */
object dap {

  def main(args: Array[String]): Unit = {


    val producerConfig: Properties = new Properties()

    producerConfig.put("schema.registry.url", "http://localhost:8081")
    producerConfig.put("bootstrap.servers", "localhost:9092")
    producerConfig.put("client.id", "Producer")
    producerConfig.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    producerConfig.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")


    //App.writeRawByte("key1")







    val arrayByte = Array[Byte](Integer.parseInt("01",16).toByte,Integer.parseInt("61",16).toByte,Integer.parseInt("72",16).toByte,Integer.parseInt("69",16).toByte,Integer.parseInt("6F",16).toByte)
    val bos = new BufferedOutputStream(new FileOutputStream("/home/dap/prova"))
    Stream.continually(bos.write(arrayByte))
    bos.close()




  }
}
