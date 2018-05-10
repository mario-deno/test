package com.sky.dap.eu_portability


import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.log4j.{LogManager, Logger}
import com.sky.dap.eu_portability.service._
import com.sky.dap.eu_portability.service.JsonService._
import com.sky.dap.eu_portability.service.CassandraActivityService._
import com.sky.dap.eu_portability.model._
import com.sky.dap.eu_portability.validator.Validator

import scala.util.{Failure, Success, Try}
import com.datastax.spark.connector._
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata




object IdentityListener {

  private val _log = LogManager.getLogger("IdentityListener")

  var cassandraKeyspace =  sys.env("CASSANDRA_KEYSPACE") //"euportability_test"
  val cassandraVipTableName = "vip_tracker"
  val cassandraActivityTableName = "activity"
  val cassandraOffsetTableName = "utils_topic_offsets"

  val topics = Set("ie.ac.ims.profile_modified", "gb.ac.ims.profile_modified")

  val kafkaParams = Map[String, String](
    "bootstrap.servers" -> "kafka-0-broker.confluent-kafka.mesos:9092,kafka-1-broker.confluent-kafka.mesos:9092,kafka-2-broker.confluent-kafka.mesos:9092,kafka-3-broker.confluent-kafka.mesos:9092,kafka-4-broker.confluent-kafka.mesos:9092",
    //"bootstrap.servers" -> "localhost:9092",
    "group.id" -> "spark-streaming-identityListener",
    "auto.offset.reset" -> "largest",
    "auto.commit.enable" -> "false"
  )


  def main(args: Array[String]) {

    // Create context with 2 second batch interval
    implicit val sparkConf = new SparkConf().setAppName("identity_listener")
      .set("spark.cassandra.connection.host", "node-0-server.cassandra.mesos")
      .set("spark.cassandra.connection.port", "9042")
      .set("spark.cassandra.connection.keep_alive_ms", "5000")
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    implicit val sc = ssc.sparkContext
    //sc.setLogLevel("DEBUG")


    _log.info("Starting...")
    _log.info(s"Cassandra keyspace: ${cassandraKeyspace}")


    val storedOffset : Map[TopicAndPartition,Long] = KafkaOffsetService.retrieveOffsets(cassandraKeyspace,cassandraOffsetTableName,kafkaParams("group.id"),topics)(sc)

    val messages = storedOffset.isEmpty match {
      case true => {
        // Unable to retrieve Offset from Cassandra or offset not found -> start from latest offsets
        _log.warn("Kafka Consumer: unable to retrieve Offset from Cassandra... start from latest offsets")
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      }
      case false => {
        // start from previously saved offsets
        _log.info("Kafka Consumer: start from previously saved offsets")
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder
          , (String, String)](ssc, kafkaParams, storedOffset, messageHandler)
      }
    }

    //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //  ssc, kafkaParams, topics)

    messages.foreachRDD { (rdd: RDD[(String, String)]) =>
      computeRDD(rdd)
    }



    // Start the computation
    ssc.start()
    ssc.awaitTermination()


    //----------------------

    //load config
    //gestione degli offsets kafka
    //define perimeter T I
    //exclude not in perimeter T I
    //deserialize message (extraction with model) T I
    //exclude failures   I
    //transformation from IMSEvent to VIP T I
    //validation filter I TODO: validare per i global access il globalAccessDate //TODO: Controllare che l'evento arrivi con configurazione gb|ie->nowtv , gb->sky
    //insert into cassandra I
    //dead letter queue produzione
    //fare un router che splitta le logiche tra EUP e GLOBALACCESS I
    //----------------------------


    /*
          successMessages.take(2).foreach({case (imsEvent,_) => println(imsEvent.event)})


          _log.info("saving...")
          val a = Vip("99999","gb","sky",10102L,8435L)
          val b = Vip("99998","gb","sky",10102L,8435L)

          val collection = sc.parallelize[Vip](Seq[Vip](a,b))

          //CassandraActivityService.saveToCassandra(collection,"euportability_test","vip_tracker")
          //CassandraActivityService.saveToCassandra(collection,"euportability_test","vip_tracker")

          collection.saveToCassandra("euportability_test","vip_tracker",SomeColumns("profile_id","home_country","provider","end_date","start_date"))

          _log.info("saved...")

          */
  }





  def computeRDD(rdd: RDD[(String, String)])(implicit sparkConf: SparkConf, sc:SparkContext): Unit = {


    if (!rdd.isEmpty()) {
      _log.info("rdd is not empty")


      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      /*for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
      } */


      _log.info(s"# received events: ${rdd.count()}")

      //marca gli eventi utili
      val markUsefulMessagesRDD  : RDD[(String, String, Boolean)]= AppMethods.markUsefulEventsRDD(rdd)


      //esclude gli eventi inutili e fa passare solo gli utili
      val usefulMessagesRDD: RDD[(String, String)] = AppMethods.excludeNotUsefulEvent(markUsefulMessagesRDD)
      usefulMessagesRDD.cache()
      _log.info(s"# useful events: ${usefulMessagesRDD.count()}")


      //deserializza sul modello
      val messages: RDD[(Try[IMS_Event], String)] = AppMethods.extractMessageRDD(usefulMessagesRDD)
      messages.cache()


      //esclude i messaggi che non è riuscito a deserializzare
      val successMessages: RDD[(IMS_Event, String)] = AppMethods.excludeFailures(messages)
      successMessages.cache()
      _log.info(s"# successfully deserialized events: ${successMessages.count()}")


      val failureMessages: RDD[(String,Throwable)] = AppMethods.getFailure(messages)
      _log.warn(s"# failed deserialized events: ${failureMessages.count()}")


      //TODO:prende i messaggi che non è riuscito a deserializzare e li manda in un RDD dedicato dlqRDD e poi in DLQ (visto che sono messaggi nel perimetro)

      //filtra tutti gli eventi di tipo globalAccess (VIP)
      val vipEvents : RDD[(IMS_Event, String)] = AppMethods.filterVipEvents(successMessages)
      vipEvents.cache()
      _log.info(s"# global access events: ${vipEvents.count()}")

      //esegue la logica per i VIP
      if(!vipEvents.isEmpty) {
        computeVIP(vipEvents)
      }

      //filtra tutti gli eventi di tipo profile_modified_EUP e con flag euportability = true (manualOverride)
      val manualOverrideEvents : RDD[(IMS_Event, String)] = AppMethods.filterEuPortabilityEvents(successMessages)
      manualOverrideEvents.cache()
      _log.info(s"# manual override events: ${manualOverrideEvents.count()}")

      //esegue la logica per la manual override
      if(!manualOverrideEvents.isEmpty()) {
        computeManualOverride(manualOverrideEvents)
      }

      KafkaOffsetService.commitOffset(cassandraKeyspace,cassandraOffsetTableName,kafkaParams("group.id"),offsetRanges)(sc)

    }
    else
      {
        _log.info("rdd is empty")
      }
  }


  def computeVIP(vipEvents: RDD[(IMS_Event, String)])(implicit sparkConf: SparkConf) = {
      //fa DQ sui messaggi ricevuti usando la funzione di DQ Validator.validateGlobalAccessEvent
      val checkValidityMessagesRDD : RDD[(IMS_Event, String, (scala.List[String], Boolean))] = AppMethods.validateEvents(vipEvents, Validator.validateGlobalAccessEvent)
      checkValidityMessagesRDD.cache()

      //esclude i non validati e fa passare i solo validati
      val validateMessagesRDD : RDD[(IMS_Event, String)] = AppMethods.excludeNotValidated(checkValidityMessagesRDD)
      validateMessagesRDD.cache()
      _log.info(s"# global access events validated: ${validateMessagesRDD.count()}")

      //TODO:prende i messaggi che non è riuscito a validare e li manda in un RDD dedicato dlqRDD e poi in DLQ (visto che sono messaggi nel perimetro)

      //converte tutti gli IMSEvent in VIP pronti per essere scritti su Cassandra
      val vipRDD : RDD[Option[Vip]] = AppMethods.IMSEvent2VIP(validateMessagesRDD)
      vipRDD.cache()

      //esclude i failures di conversione modello ... puo succedere (ma non dovrebbe) che nella conversione ImsEvent -> Vip qualcosa non vada a buon fine
      val vipConversionSuccessRDD : RDD[Vip] = AppMethods.excludeConversionFailure(vipRDD)
      _log.info(s"# global access events -> VIP: ${vipConversionSuccessRDD.count()}")
      vipConversionSuccessRDD.cache()

      //TODO:prende i messaggi che non è riuscito a convertire e li manda in un RDD dedicato dlqRDD e poi in DLQ (visto che sono messaggi nel perimetro)
      val vipConversionFailureRDD : RDD[Vip] = AppMethods.getConversionFailure(vipRDD)
      if (!vipConversionFailureRDD.isEmpty()) _log.warn(s"# global access events -> VIP failed: ${vipConversionFailureRDD.count()}")

      //scrive i dati su Cassandra
      if (!vipConversionSuccessRDD.isEmpty())
      {
        CassandraActivityService.saveVipToCassandra(vipConversionSuccessRDD, cassandraKeyspace, cassandraVipTableName)
        _log.info(s"# VIP inserted into Cassandra table: ${vipConversionSuccessRDD.count()}")
      }
      else
        _log.info(s"no VIP inserted into Cassandra table")
  }

  def computeManualOverride(manualOverrideEvents: RDD[(IMS_Event, String)]) =
  {
    //fa DQ sui messaggi ricevuti usando la funzione di DQ Validator.validateEuPortabilityEvent
    val checkValidityMessagesRDD : RDD[(IMS_Event, String, (scala.List[String], Boolean))] = AppMethods.validateEvents(manualOverrideEvents, Validator.validateEuPortabilityEvent)

    //esclude i non validati e fa passare i solo validati
    val validateMessagesRDD : RDD[(IMS_Event, String)] = AppMethods.excludeNotValidated(checkValidityMessagesRDD)
    validateMessagesRDD.cache()
    _log.info(s"# manual override events validated: ${validateMessagesRDD.count()}")


    //converte tutti gli IMSEvent in Activity pronti per essere scritti su Cassandra
    val activityRDD : RDD[Option[Activity]] = AppMethods.IMSEvent2Activity(validateMessagesRDD)
    activityRDD.cache()
    _log.info(s"# manual override -> Activity: ${activityRDD.count()}")

    //Estrae l'option perchè per adesso la conversione è semplice quindi non ci saranno None
    val activityConversionSuccessRDD: RDD[Activity] = activityRDD.map(_.get)
    activityConversionSuccessRDD.cache()


    //cancella le attività su Cassandra per effetto della manual override
    if (!activityConversionSuccessRDD.isEmpty()) {
      CassandraActivityService.deleteActivityToCassandra(activityConversionSuccessRDD, cassandraKeyspace, cassandraActivityTableName)
      _log.info(s"# Activities delete from Cassandra table: ${activityConversionSuccessRDD.count()}")
    }
    else
      _log.info(s"no Activities deleted from Cassandra table")
  }



  }



object AppMethods {

    def filterVipEvents(rdd: RDD[(IMS_Event, String)]) : RDD[(IMS_Event, String)] =
    {
      val vipEvents = rdd.filter(event => event._1.event.toUpperCase.contains("PROFILE_MODIFIED_GLOBAL_ACCESS"))
      vipEvents
    }


    // Given a RDD[kafkakey,kafkavalue] return a Rdd[kafkakey,kafkavalue,boolean value,event type ] where boolean value is true if is an IMSevent we are interested on (ex. event = "PROFILE_MODIFIED_GLOBAL_ACCESS") and event type is the value of the event type
    def markUsefulEventsRDD( rdd : RDD[(String, String)] ) : RDD[(String, String, Boolean)]  = {
      rdd.map( { case ( k , v ) => (k,v , matchWithRules(v))})
    }

    def matchWithRules ( json : String ) : Boolean = {

      val fieldName = "event"
      val interestedEventType : List[String] = List("PROFILE_MODIFIED_GLOBAL_ACCESS","PROFILE_MODIFIED_EUP")
      val result = Try(extractFieldFromJson(fieldName, json))
      result match {
        case Success(v) => if (!v.isEmpty && interestedEventType.contains(v.get.toUpperCase)) true else false
        case Failure(e) => false
      }
    }


    def excludeNotUsefulEvent(rdd: RDD[(String, String, Boolean)]) : RDD[(String, String)] =
    {
      val rddResult = rdd.filter(inputRdd => inputRdd._3).map(interestedEvent => (interestedEvent._1,interestedEvent._2))
      rddResult
    }


    //given an Rdd[key,value] from kafka return a RDD[Try(message deserialized,) original_json]
    def extractMessageRDD(rdd: RDD[(String, String)])(implicit sparkConf: SparkConf): RDD[(Try[IMS_Event], String)] = {
      rdd.map({ case (kafkaKey, kafkaValue) =>
        val msg = extractMessage(kafkaValue)
        (msg, kafkaValue)
      })
    }

    //for a given kafkaValue:String try to deserialize into IMSEvent
    def extractMessage(value: String): Try[IMS_Event] = {
      Try(JsonService.extractMessageFromJson[IMS_Event](value).get)
    }

    //given a RDD:(Try[IMS_Event], String) return the same RDD but where the model is equal to Success (model has been applied successfully)
    def excludeFailures(messages: RDD[(Try[IMS_Event], String)]): RDD[(IMS_Event, String)] = {
      messages.map({ case (model, raw_message) => (model.getOrElse(null), raw_message) })
        .filter({ case (model, _) => model != null })
    }


    //given a RDD:(Try[IMS_Event], String) return the RDD where first element is the original message and the second is the throwable generated (root cause of the failure)
    def getFailure(messages: RDD[(Try[IMS_Event], String)]): RDD[(String, Throwable)] =
    {
      messages.filter(event => event._1.isFailure).map(event => (event._2,event._1.failed.get))
    }


    def IMSEvent2VIP(messages: RDD[(IMS_Event, String)]): RDD[Option[Vip]] = {
      messages.map({ case (model, _) => ModelConverter.IMSEvent2Vip(model) })
    }


    //performs some DQ on messagesExtracted using f as Data quality function
    def validateEvents(messagesExtracted: RDD[(IMS_Event, String)] , dqFunc: IMS_Event => (List[String],Boolean)): RDD[(IMS_Event, String, (List[String],Boolean))] = {
      messagesExtracted.map({ case (deserializedEvent, rawJsonMessage) => (deserializedEvent, rawJsonMessage, dqFunc(deserializedEvent))})
    }

    def excludeNotValidated(rdd: RDD[(IMS_Event, String, (scala.List[String], Boolean))]) : RDD[(IMS_Event, String)] =
    {
      rdd.filter({case (imsEvent,rawJsonMessage,(errorList,validated)) => validated}).map({case (imsEvent,rawJsonMessage,(_,_)) => (imsEvent,rawJsonMessage)})
    }

    def excludeConversionFailure(rdd:RDD[Option[Vip]]) : RDD[Vip] =
    {
      rdd.filter(x => !x.isEmpty).map(x => x.get)
    }

    def getConversionFailure(rdd:RDD[Option[Vip]]) : RDD[Vip] =
    {
      rdd.filter(x => x.isEmpty).map(x => x.getOrElse(Vip("","","",0,0)))
    }


    def filterEuPortabilityEvents(inputRdd: RDD[(IMS_Event, String)]): RDD[(IMS_Event, String)] =
    {
      val vipEvents = inputRdd.filter(event => event._1.event.toUpperCase.contains("PROFILE_MODIFIED_EUP") && event._1.data.profile.euportability)
      vipEvents
    }

    def IMSEvent2Activity(validateMessagesRDD: RDD[(IMS_Event, String)]): RDD[Option[Activity]] = {
      validateMessagesRDD.map{
        case (model, _) => ModelConverter.IMSEvent2Activity(model)
      }
    }


}