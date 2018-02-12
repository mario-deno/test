package com.sky.dap.soc.detector

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import com.datastax.spark.connector.writer.{BatchGroupingKey, WriteConf}
import com.sky.dap.soc.detector.entity.{StartOfCredits, StreamStop}
import com.sky.dap.soc.detector.service.SocUpdatesPublisher
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

/**
  * Created by dreminiv on 18/07/2017.
  */
object StartOfCreditsDetector {
  private val logger = LoggerFactory.getLogger(StartOfCreditsDetector.getClass)

  def readEventsFromHdfs(hdfsBasePath: String, hdfsParquetPath: List[String], startDateString: String, endDateString: String)
                        (implicit spark: SparkSession): DataFrame = {
    val parquetAC = spark.read
      .option("basePath", hdfsBasePath)
      .option("mergeSchema", "true")
      .parquet(hdfsParquetPath: _*)
      .select(StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition, StreamStop.videoId, StreamStop.streamPosition, StreamStop.activityType, StreamStop.activityDate)
      .filter(col(StreamStop.activityDate) > startDateString and col(StreamStop.activityDate) <= endDateString)
      .filter(col(StreamStop.activityType) === "STREAM_STOP")
//      .filter(col(StreamStop.proposition) === "NOWTV")
      .withColumn(StreamStop.streamPosition, col(StreamStop.streamPosition).cast("long"))
      .filter(col(StreamStop.streamPosition) > 0)
      .na.drop(Seq(StreamStop.streamPosition, StreamStop.videoId, StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition)) // delete null values
    parquetAC

  }

  def filterSignificantContribution(streamStopDf: DataFrame, eventsThresholdFromArgs: Int)
                                   (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val windowPositionCount = Window.partitionBy(col(StreamStop.provider), col(StreamStop.providerTerritory), col(StreamStop.proposition), col(StreamStop.videoId)).orderBy(StreamStop.streamPosCount)
      .rangeBetween(Long.MinValue, Long.MaxValue)

    val withTotalContribution = streamStopDf.withColumn("totalContribution", sum(col(StreamStop.streamPosCount)).over(windowPositionCount))

    val filteredSignificantContribution = withTotalContribution.filter($"totalContribution" > eventsThresholdFromArgs)
      .drop("totalContribution")
    filteredSignificantContribution

  }

  def countPositions(df: DataFrame)
                    (implicit spark: SparkSession): DataFrame = {
    val countedStreamPosition = df.groupBy(StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition, StreamStop.videoId, StreamStop.streamPosition)
      .count()
      .withColumnRenamed("count", StreamStop.streamPosCount)

    countedStreamPosition
  }

  def filterPerCountWithLastPosition (df: DataFrame, filterCount: Int)
                                     (implicit spark: SparkSession): DataFrame ={

    val windowPositionDesc = Window.partitionBy(col(StreamStop.provider), col(StreamStop.providerTerritory), col(StreamStop.proposition), col(StreamStop.videoId))
      .orderBy(col(StreamStop.streamPosition).desc)
    //here rangeBetween is not specified. Spark create a window of not-determinate bound. In this case i dont care size of the windows because i get first record

    val withLastPosition = df.filter(col(StreamStop.streamPosCount)>filterCount)
      .withColumn("lastPosition", first(StreamStop.streamPosition).over(windowPositionDesc))

    withLastPosition

  }

  def aggregateLocalSum (df: DataFrame, aggregateInterval: Int)
                        (implicit spark: SparkSession): DataFrame = {

    val windowPosRange =  Window.partitionBy(StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition, StreamStop.videoId).orderBy(col(StreamStop.streamPosition).asc)
      .rangeBetween(0, aggregateInterval)


    val aggregated = df.withColumn("localSum", sum(StreamStop.streamPosCount).over(windowPosRange) )

    aggregated

  }

  def computeTopK(streamStopSignificantDf: DataFrame, topK: Int, topKColumnName: String)
                 (implicit spark: SparkSession): DataFrame = {


    val windowTopK = Window.partitionBy(StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition, StreamStop.videoId).orderBy(col(topKColumnName).desc)

    val orderedByCount = streamStopSignificantDf.withColumn(StreamStop.windowRank, row_number().over(windowTopK))

    val topKDF = orderedByCount.where(col(StreamStop.windowRank) <= topK)

    topKDF
  }

  def computeTopKWithoutEoaAndPositionPortion(df: DataFrame, topK: Int, filterPositionPortion: Double)
                                             (implicit spark: SparkSession): DataFrame = {


    val windowPositionAsc = Window.partitionBy(StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition, StreamStop.videoId).orderBy(col(StreamStop.streamPosition).asc, col("localSum"))
    //localsum used in ordering only by technical reason because windows function works considering this field. So i need to insert it


    //i am a local max if my previous and following stream position have a localsum less than mine
    //localMaxFiltered contains only localmax spikes
    val localMaxFiltered = df.withColumn("isLocalMax", lag("localSum", 1, 0).over(windowPositionAsc)<col("localSum") and col("localSum")>=  lead("localSum", 1, 0).over(windowPositionAsc))
      .filter(col("isLocalMax"))

    //get top k+1 spikes (why k+1? because later one of these is assumed to be the EoA and then it will be eliminated... so finally we consider only the K top Spikes)
    val topKp1Df = computeTopK(localMaxFiltered, topK+1, "localSum")

    val windowPositionDesc = Window.partitionBy(StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition, StreamStop.videoId).orderBy(col(StreamStop.streamPosition).desc)

    //take into consideration only the stream position < of the last stream position
    // delete maximum streamPosition spike (assumed to be the EoA dependent by paramenter K) .It is a strong assumption
    val filteredEoa = topKp1Df.withColumn("EoaPosition", first(StreamStop.streamPosition).over(windowPositionDesc)).filter(col(StreamStop.streamPosition)<col("EoaPosition"))

    //take into consideration only the spike with streamPosition > filterPositionPortion(0.7 now) * streamPosition of the last spike (deleted before)
    //so we consider only spikes in the last filterPositionPortion% portion of the video
    //then overwrite the streamPosValue with localsum value
    //why 0..7*streamPosition of the last spike??? this because we assumed the real Soc is very near to EoA
    val filteredDf =  filteredEoa.withColumn(StreamStop.streamPosCount, col("localSum"))
      .filter(col(StreamStop.streamPosition) > col("EoaPosition")*lit(filterPositionPortion))

    filteredDf

  }

  def safeToParquet(socTopkDf: DataFrame, hdfsParquetWrite: String)(implicit spark: SparkSession): Unit = {
    socTopkDf.withColumn("timestamp", unix_timestamp())
      .write
      .partitionBy("timestamp")
      .mode(SaveMode.Append)
      .parquet(hdfsParquetWrite)
  }

  def computeConfidenceValue(socTopkDf: DataFrame, powerFromArgs: Int)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._
    val windowCountUnbounded = Window.partitionBy(col(StreamStop.provider), col(StreamStop.providerTerritory), col(StreamStop.proposition), col(StreamStop.videoId))
      .orderBy(col(StreamStop.streamPosCount).desc)
      .rangeBetween(Long.MinValue, Long.MaxValue)




    val windowPosCount = Window.partitionBy(StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition, StreamStop.videoId).orderBy(col(StreamStop.streamPosCount).desc)

    val positionWeight = {
      val socPosition = first(col(StreamStop.streamPosition)).over(windowCountUnbounded)


      val farthestMin = socPosition - min(col(StreamStop.streamPosition)).over(windowCountUnbounded)
      val farthestMax = max(col(StreamStop.streamPosition)).over(windowCountUnbounded) - socPosition



      val maxUdf = udf((left: Long, right: Long) => Math.max(left, right))

      //calculate maximum distance (based on streamPosition)  beetween the ipotetic soc (with localsum Max) and first and last spike
      val biggestDiff = maxUdf(farthestMin, farthestMax)


      //we divide for biggestDiff to normalize in [0,1] the weight. No one can be most far than biggestDiff
      //for each stream position if it is near to the ipotetic Soc the weight is lower. Viceversa the weight is bigger if it is far away from ipotetic Soc
      //Soc has weight = 0
      val toPower = abs(col(StreamStop.streamPosition) - socPosition) / biggestDiff

      //to increase the weight in a non linear way
      val weight = pow(toPower, powerFromArgs)
      weight
    }

    //calculate the positionWeigth for each spike. PositionWeigth measure the distance from the Soc
    val withWeights = socTopkDf.withColumn("positionWeight", positionWeight)
    //weightedCount = multiplicate the positionWeight for the localSum
    val withWeightedCounts = withWeights.withColumn("weightedCount", $"positionWeight" * col(StreamStop.streamPosCount))

    //i want obtain a weighted mean of the localsum and measure the distance from the localSum of the Soc and the weighted mean of the localsum
    //if the Soc localSum is near to the weighted mean my confidence is low -> i don't know if i am discriminating the Soc very better
    //it the Soc localsum is far away from the weighted mean (this implies other spikes are very short or very far) and i0m very confident that my Soc is the correct One
    val confidenceValue = {
      val weightedAverage = sum($"weightedCount").over(windowCountUnbounded) / sum($"positionWeight").over(windowCountUnbounded)
      val soc = max(col(StreamStop.streamPosCount)).over(windowCountUnbounded)
      val confidenceValue = (soc - weightedAverage) / soc // standardize the confidenceValue
      confidenceValue
    }

    //for each provider, providerTerritory, proposition, videoId return only the choosen one Soc
    val withConfidenceValue = withWeightedCounts.withColumn("confidenceValue", confidenceValue)
      .withColumn(StreamStop.windowRank, row_number().over(windowPosCount))
      .filter(col(StreamStop.windowRank) === 1).drop("positionWeight").drop("weightedCount")

    withConfidenceValue
  }


  def setSocSchemaExpr(socConfValueDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val newSocDf = socConfValueDf
      .selectExpr(
        StreamStop.providerTerritory + " as " + StartOfCredits.providerTerritory,
        StreamStop.provider + " as " + StartOfCredits.provider,
        StreamStop.proposition + " as " + StartOfCredits.proposition,
        StreamStop.videoId + " as " + StartOfCredits.videoId,
        StreamStop.streamPosition + " as " + StartOfCredits.position,
        StreamStop.confidenceValue + " as " + StartOfCredits.confidence
      )
    newSocDf
  }


  def readSocFromCassandra(table: String, keyspace: String)
                          (implicit spark: SparkSession): DataFrame = {
    val socFromCassandra = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .load()
      .withColumnRenamed(StartOfCredits.position, "lastPosition")
      .withColumnRenamed(StartOfCredits.confidence, "lastConfidence")
    socFromCassandra
  }


  def computeSocToUpdate(newSocDf: DataFrame, socFromCassandraDf: DataFrame, significantDifferenceFromArgs: Int)
                        (implicit spark: SparkSession): DataFrame = {

    import spark.implicits._
    val joinedSocDf = newSocDf.join(socFromCassandraDf, Seq(StartOfCredits.providerTerritory, StartOfCredits.provider, StartOfCredits.proposition, StartOfCredits.videoId), "left_outer")

    val filterCondition = (abs(col(StartOfCredits.position) - $"lastPosition") > significantDifferenceFromArgs
      or $"lastPosition".isNull)

    def getUtcZoneTimestamp():String ={
      Instant.now().atZone(ZoneId.of("Etc/UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
    }

    val socToUpdateDf = joinedSocDf.filter(filterCondition)
      .selectExpr(StartOfCredits.providerTerritory, StartOfCredits.provider, StartOfCredits.proposition, StartOfCredits.videoId, StartOfCredits.position,
        StartOfCredits.confidence)
      .withColumn(StartOfCredits.lastupdated, lit(getUtcZoneTimestamp()))
        .na.fill(1, Seq(StartOfCredits.confidence))
    socToUpdateDf

  }


  def upsertSocToCassandra(table: String, keyspace: String, socToUpdateDf: DataFrame)
                          (implicit spark: SparkSession): Unit = {
    socToUpdateDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .mode(SaveMode.Append)
      .save()
  }

  def upsertSocToCassPart(table: String, keyspace: String, socToUpdateDf: DataFrame)
                         (implicit spark: SparkSession): Boolean = {
    val partitionNumber = socToUpdateDf.rdd.getNumPartitions
    socToUpdateDf.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .options(WriteConf.BatchLevelParam.sqlOption(BatchGroupingKey.Partition)
        ++ WriteConf.ParallelismLevelParam.sqlOption(partitionNumber))
      .mode(SaveMode.Append)
      .save()
    true
  }

  def sendToKafka(topics: Set[String], socToUpdateDf: DataFrame)
                 (implicit spark: SparkSession): Boolean = {

    val socToUpdateRDD = SocUpdatesPublisher.convertDfToRDD(socToUpdateDf)
    SocUpdatesPublisher.publish(socToUpdateRDD, topics)
    true
  }

  /*
  Deprecated
   */
  def setSocSchema(socConfValueDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val newSocDf = socConfValueDf
      .withColumnRenamed(StreamStop.providerTerritory, StartOfCredits.providerTerritory)
      .withColumnRenamed(StreamStop.provider, StartOfCredits.provider)
      .withColumnRenamed(StreamStop.proposition, StartOfCredits.proposition)
      .withColumnRenamed(StreamStop.streamPosition, StartOfCredits.position)
      .withColumnRenamed(StreamStop.confidenceValue, StartOfCredits.confidence)
      .drop(StreamStop.streamPosCount)
      .drop(StreamStop.windowRank)

    newSocDf
  }


  /*
  Deprecated
   */

  def computeTopKPositions(streamStopSignificantDf: DataFrame, topK: Int)
                          (implicit spark: SparkSession): DataFrame = {
    val countedStreamPosition = streamStopSignificantDf.groupBy(StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition, StreamStop.videoId, StreamStop.streamPosition)
      .count()
      .withColumnRenamed("count", StreamStop.streamPosCount)


    // countedStreamPosition.printSchema()

    val windowCount = Window.partitionBy(StreamStop.provider, StreamStop.providerTerritory, StreamStop.proposition, StreamStop.videoId).orderBy(col(StreamStop.streamPosCount).desc)

    val orderedByCount = countedStreamPosition.withColumn(StreamStop.windowRank, row_number().over(windowCount))

    val socTopK = orderedByCount.where(col(StreamStop.windowRank) <= topK)
    socTopK
  }

}
