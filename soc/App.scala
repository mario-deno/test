package com.sky.dap.soc.detector

import java.time.LocalDate

import com.sky.dap.soc.detector.config._
import com.sky.dap.soc.detector.consts.SparkParams
import com.sky.dap.soc.detector.entity.StreamStop
import com.sky.dap.spark.kafka.connector.{KafkaProducerBrokers, KafkaProducerSchemaRegistry}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object App {
  private val logger = LoggerFactory.getLogger(App.getClass)


  private val sparkConfig = SparkConfig.getConfig()
  private val kafkaConfig = KafkaConfig.getConfig()
  private val dBConfig = DBConfig.getConfig()


  def main(args: scala.Array[String]): Unit = {
    // get arguments
    val startFromDate = args(0).toString


    val currentDate = {
      if (startFromDate == "now") LocalDate.now()
      else LocalDate.parse(startFromDate)
    }

    val endDateString = currentDate.toString

    val numberDaysBack = args(1).toLong

    val startDate = currentDate.minusDays(numberDaysBack)

    val startDateString = startDate.toString()

    //logger.info(s"Compute SoC for the period from ${startDateString} to ${endDateString}")

    val eventsThresholdFromArgs = args(2).toInt

    //logger.info(s"Filter out videoids with total events less than ${eventsThresholdFromArgs}")

    val topK = args(3).toInt

    val powerFromArgs = args(4).toInt

    val significantDifferenceFromArgs = args(5).toInt

    val significantCountThreshold = args(6).toInt //1

    val aggregationStep = args(7).toInt //3

    val positionPortionThreshold =  args(8).toDouble //0.7

    //logger.info(s"Compute confidence value from Top${topK} elements using pow(x, ${powerFromArgs}) as a weight distribution function ")

    implicit val spark = SparkSession
      .builder()
      .appName(sparkConfig.appName)
      .config(SparkParams.CASSANDRA_CONNECTION_HOST, dBConfig.host)
      .config(SparkParams.CASSANDRA_CONNECTION_PORT, dBConfig.port)
      .config(KafkaProducerBrokers.name, kafkaConfig.bootstrapServers)
      .config(KafkaProducerSchemaRegistry.name, kafkaConfig.schemaRegistryUrl)
      .getOrCreate()

//    spark.sparkContext.getConf.set(KafkaProducerBrokers.name, kafkaConfig.bootstrapServers)
//    spark.sparkContext.getConf.set(KafkaProducerSchemaRegistry.name, kafkaConfig.schemaRegistryUrl)

    //return a filtered dataFrame (activityDate  between startDateString to endDateString, streamposition >0, not null etc..) with the following structure:
    //((videoId, streamPosition, activityType, activityDate, proposition)
    val streamStopDf = StartOfCreditsDetector.readEventsFromHdfs(sparkConfig.hdfsBasePath,
      sparkConfig.hdfsParquetRead, startDateString, endDateString)



    //save i memory the dataframe tyo avoid laziness
    streamStopDf.cache()
    //streamStopDf.createOrReplaceTempView("test_period_table")



    logger.info(s"Compute SoC for the period from ${startDateString} to ${endDateString}")
    logger.info(s"Filter out videoids with total events less than ${eventsThresholdFromArgs}")
    logger.info(s"Compute confidence value from Top${topK} elements using pow(x, ${powerFromArgs}) as a weight distribution function ")

    logger.info(s"Stream Stop events to elaborate count: ${streamStopDf.count()}")

    //for each (provider, providerTerritory, proposition, videoId, streamPosition) count number of rows
    //so for each video and stop event count the events gathered
    //resulting dataframe: (provider, providerTerritory, proposition, videoId, streamPosition,streamPosCount)
    val countedStreamPositionDf =  StartOfCreditsDetector.countPositions(streamStopDf)

    //delete all video  having stop events below a given threshold (eventsThresholdFromArgs passed in input)
    val filteredSignificantDf = StartOfCreditsDetector.filterSignificantContribution(countedStreamPositionDf, eventsThresholdFromArgs)

    //for each video mark the last streamPosition with stop events (streamPosCount) > significantCountThreshold
    //resulting dataframe (provider, providerTerritory, proposition, videoId, streamPosition,streamPosCount,lastPosition(new field with last interesting streamposition))
    //don't delete any rows
    //it should not be the End of Asset or Start of Credit, because streamPosCount of EoA or SoC can be below of significantCountThreshold
    val filteredPerCountWithLastPositionDf = StartOfCreditsDetector.filterPerCountWithLastPosition(filteredSignificantDf, significantCountThreshold)

    filteredPerCountWithLastPositionDf.cache
    filteredPerCountWithLastPositionDf.count


    // for each provider, providerTerritory, proposition, videoId calculates local sum
    //local sum is the sum of the StreamPosCount of the next three streamPosition
    //(provider, providerTerritory, proposition, videoId, streamPosition,streamPosCount,lastPosition, localsum)
    val aggregatedDf = StartOfCreditsDetector.aggregateLocalSum(filteredPerCountWithLastPositionDf, aggregationStep)


    // top k spikes excluding EoA spike(assumpted) and spikes very far from EoA
    val topKDf = StartOfCreditsDetector.computeTopKWithoutEoaAndPositionPortion(aggregatedDf, topK, positionPortionThreshold)

    topKDf.cache()
    topKDf.count()

    //No here we are working with streamPosCount = localSum

    //elects the Soc and performs calculation of its own confidence value
    val confidenceValueDf = StartOfCreditsDetector.computeConfidenceValue(topKDf, powerFromArgs)


    //from here we compare tfor each video if the Soc is changed respect the Soc stored into Cassandra tables
    //if the Soc has changed we update the value of The Soc and the produce a message towards an internal topic

    val newSocDf = StartOfCreditsDetector.setSocSchemaExpr(confidenceValueDf)


    //val videos = topKDf.select(StreamStop.videoId).distinct().count()
    //logger.info(s"Saving top ${topK} positions for ${videos} videos to HDFS")

    //StartOfCreditsDetector.safeToParquet(topKDf, sparkConfig.hdfsParquetWrite)

    //logger.info(s"Saved top ${topK} positions to HDFS")


    val socFromCassandraDf = StartOfCreditsDetector.readSocFromCassandra("soc", dBConfig.keyspace)

    val socToUpdateDf = StartOfCreditsDetector.computeSocToUpdate(newSocDf, socFromCassandraDf, significantDifferenceFromArgs)

    socToUpdateDf.cache()

    logger.info(s"SoC to update count: ${socToUpdateDf.count()}")

    //StartOfCreditsDetector.upsertSocToCassandra("soc", dBConfig.keyspace, socToUpdateDf)

    val success = StartOfCreditsDetector.upsertSocToCassPart("soc", dBConfig.keyspace, socToUpdateDf)

    logger.info(s"SoC upsert to Cassandra succeeded ${success}")

    //TODO: implement batch upsert together with kafka publishing to insure fail tolerance
    StartOfCreditsDetector.sendToKafka(kafkaConfig.socUpdateTopics, socToUpdateDf)


    spark.stop()

  }


}
