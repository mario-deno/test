package com.sky.dap.eu_portability.service

import com.sky.dap.eu_portability.model.KafkaOffset
import kafka.common.TopicAndPartition
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.OffsetRange

import scala.util.{Failure, Success, Try}

object KafkaOffsetService {

  private val _log = LogManager.getLogger("KafkaOffsetService")

  def retrieveOffsets(cassandraKeyspace : String , cassandraOffsetTableName: String , groupId: String, topics: Set[String])(implicit sc: SparkContext): Map[TopicAndPartition, Long] = {

    //read from cassandra table the offsets
    val rdd = Try(CassandraActivityService.readOffsetFromCassandra(cassandraKeyspace,cassandraOffsetTableName)(sc))
    rdd match {
      case Success(rdd) =>{
        //filter only the groupId and topics we need
        val filteredOffsetRecordsRDD : RDD[KafkaOffset] = rdd.filter(x => topics.map(x => x.toUpperCase).contains(x.topic.toUpperCase) && x.group_id.toUpperCase.contains(groupId.toUpperCase))
        //prepare an RDD and trasforms it to a map
        val result : RDD[(TopicAndPartition, Long)] = filteredOffsetRecordsRDD.map(x => (TopicAndPartition(x.topic,x.partition) -> x.next_offset))
        result.collect().toMap //return data to the driver
      }
      case Failure(e) => {
        _log.error(e.getStackTrace.toString)
        Map.empty
      }
    }
  }


  def commitOffset(cassandraKeyspace : String , cassandraOffsetTableName: String , groupId: String, offsets: Array[OffsetRange])(implicit sc: SparkContext): Unit = {


    var offset = Array[KafkaOffset]()

    offsets.foreach(offsetRange => {
      val topic = offsetRange.topic
      val partition = offsetRange.partition
      val next_offset = offsetRange.untilOffset

      offset +:= KafkaOffset(groupId,topic,partition,next_offset)

    })

    val rdd : RDD[KafkaOffset] = sc.parallelize[KafkaOffset](offset)

    val result = Try(CassandraActivityService.saveOffsetFromCassandra(rdd,cassandraKeyspace,cassandraOffsetTableName)(sc))
    result match {
      case Success(v) =>{
        _log.info("Kafka offset committed successfully to Cassandra")
      }
      case Failure(e) => {
        _log.error(e.getStackTrace.toString)

      }
    }

  }

}
