package com.sky.dap.eu_portability.unit.manual_override

import com.sky.dap.eu_portability.AppMethods._
import com.sky.dap.eu_portability.SparkFactory._
import com.sky.dap.eu_portability.UnitSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.sky.dap.eu_portability.data.IMSEvents
import com.sky.dap.eu_portability.model.{Activity, IMS_Event}
import com.sky.dap.eu_portability.service.JsonService
import com.sky.dap.eu_portability.validator.Validator

import scala.util.{Success, Try}

class MOV_UnitTest extends UnitSpec("UnitTest") {

  // TODO : Externalize parameters
  // ************************************
  val env = "dev"
  val cassandraHost = "localhost"
  val cassandraPort = "9042"
  // ************************************

  getSparkWithCassandra(SPARK_APP_NAME, SPARK_MASTER, cassandraHost, cassandraPort)

  val keyspace = "euportability_" + env
  val tableName = "activity"

  implicit val sc = spark.sparkContext
  implicit val conf = sc.getConf

  def createRDDfromJSON(json: String, keyJson: String = "999"): RDD[(String, String)] = {
    val arrayJson = Array((keyJson, json))
    val rdd = sc.parallelize(arrayJson)
    rdd
  }


  def createRDDfromJsonToIMS(json: String): RDD[(IMS_Event, String)] = {
    val arrayIMSJson = Array((JsonService.extractMessageFromJson[IMS_Event](json).get, json))
    val rdd = sc.parallelize(arrayIMSJson)
    rdd
  }

  val topic = "topic_name"

  /*
  // define perimeter
  "define Manual Override perimeter method" when {

    "json has not event field" must {

      val rdd = createRDDfromJSON(IMSEvents.eventFake._2)

      "event is not in perimeter" in {
        val rddResult : RDD[(String, String, Boolean)]= markUsefulEventsRDD(rdd)
        val result = rddResult.first()

        result._3 shouldBe false
      }
    }

    "json is empty" must {

      val rdd = createRDDfromJSON("") //empty body

      val rddResult : RDD[(String, String, Boolean)]= markUsefulEventsRDD(rdd)
      val result = rddResult.first()

      "event is not in perimeter" in {
        result._3 shouldBe false
      }
    }


    "json has event field" when {
      "event is NOT PROFILE_MODIFIED_EUP" must {
        "is not in perimeter" in {
          val rdd = createRDDfromJSON(IMSEvents.eventNotInPerimeter._2)

          val rddResult: RDD[(String, String, Boolean)] = markUsefulEventsRDD(rdd)
          val result = rddResult.first()

          result._3 shouldBe false
        }
      }
      "event is PROFILE_MODIFIED_EUP" must {
        "and EU_PORTABILITY Flag active, is in perimeter" in {
          val rdd = createRDDfromJSON(IMSEvents.eventInPerimeter._2)

          val rddResult: RDD[(String, String, Boolean)] = markUsefulEventsRDD(rdd)
          val result = rddResult.first()

          result._3 shouldBe true
        }
        "and EU_PORTABILITY Flag NOT active, is in NOT perimeter" in {
          val rdd = createRDDfromJSON(IMSEvents.eventInPerimeterAndEuPortabilityNotActive._2)

          val rddResult: RDD[(String, String, Boolean)] = markUsefulEventsRDD(rdd)
          val result = rddResult.first()

          result._3 shouldBe false
        }
      }
    }
  }


  //exclude not in perimeter
  "exclude events not in perimeter" in {pending}

  //extractMessageRDD_ManualOverride
  "extract message" when {
    "event is compatible with model" must {
      val rdd = createRDDfromJSON(IMSEvents.eventCompatible._2)
      val rddResult : RDD[(Try[IMS_Event], String)] =  extractMessageRDD_ManualOverride(rdd, topic)
      val result = rddResult.first()

      "is success" in {
        result._1.isSuccess shouldBe true
      }
      "profileID is correct" in {pending}
    }

    "event is NOT compatible with model" must {
      val rdd = createRDDfromJSON(IMSEvents.eventNotCompatible._2)
      val rddResult : RDD[(Try[IMS_Event], String)] =  extractMessageRDD_ManualOverride(rdd, topic)
      val result = rddResult.first()

      "is failure" in {
        result._1.isFailure shouldBe true
      }
    }
  }

  //excludeFailuresManualOverride
  "exclude Extraction failures" must {
    val rdd = createRDDfromJSON(IMSEvents.eventValid._2)
    val rddExtracted  = extractMessageRDD_ManualOverride(rdd, topic)
    val rddFiltered   = excludeFailuresManualOverride(rddExtracted).persist()

    "event has passed extraction and not excluded" in {rddFiltered.count() shouldBe 1}

  }


  //validationEventsManualOverride
  "validate events" when {
    "event is valid" must {
      val rdd = createRDDfromJSON(IMSEvents.eventValid._2)
      val rddExtracted = extractMessageRDD_ManualOverride(rdd, topic)
      val rddFiltered = excludeFailuresManualOverride(rddExtracted)
      val rddValidation = validationEventsManualOverride(rddFiltered).persist()
      val rddResult = rddValidation.first()

      "event has passed extraction and validation" in {rddValidation.count() shouldBe 1}
      "event passed" must {
        val model: IMS_Event = rddResult._1
        val errorList = rddResult._3

        "model is correct" in {
          model.data.profile shouldBe  "" // TODO : Improve this test
        }
        "has errorList empty" in {errorList._2.isEmpty}
        "has flag True ( validated )" in {errorList._1 shouldBe true}
      }

    }

    "event is not valid" must {
      val rdd = createRDDfromJSON(IMSEvents.eventNotValid._2)
      val rddExtracted = extractMessageRDD_ManualOverride(rdd, topic)
      val rddFiltered = excludeFailuresManualOverride(rddExtracted)
      val rddValidation = validationEventsManualOverride(rddFiltered).persist()
      val rddResult = rddValidation.first()

      "event has passed extraction and validation" in {rddValidation.count() shouldBe 1}
      "event passed" must {
        val model: IMS_Event = rddResult._1
        val errorList = rddResult._3

        "has errorList not empty" in {!errorList._2.isEmpty}
        "has errorList with Errors" in {errorList._2.contains("") shouldBe true} // TODO : Insert Values of Error List
        "has flag False ( not validated )" in {errorList._1 shouldBe false}
      }
    }
  }

  //validatedEventsFilterManualOverride
  "filter validate events" when {
    "event is valid" must {
      val rdd = createRDDfromJSON(IMSEvents.eventValid._2)
      val rddExtracted = extractMessageRDD_ManualOverride(rdd, topic)
      val rddFiltered = excludeFailuresManualOverride(rddExtracted)
      val rddValidation = validationEventsManualOverride(rddFiltered).persist()
      val rddResult = validatedEventsFilterManualOverride(rddValidation).persist()
      "event has passed extraction and validation and filtered as Validated" in {rddResult.count() shouldBe 1}
      "event filtered" must {
        val model: IMS_Event = rddResult.first()

        "model is correct" in {
          model.data.profile shouldBe "" // TODO : Improve this test
        }
      }
    }
    "event is not valid" must {
      val rdd = createRDDfromJSON(IMSEvents.eventNotValid._2)
      val rddExtracted = extractMessageRDD_ManualOverride(rdd, topic)
      val rddFiltered = excludeFailuresManualOverride(rddExtracted)
      val rddValidation = validationEventsManualOverride(rddFiltered).persist()
      val rddResult = validatedEventsFilterManualOverride(rddValidation).persist()
      "rdd is empty" in {rddResult.isEmpty() shouldBe true}
    }
  }

*/



  val rddVIPEvent = createRDDfromJsonToIMS(IMSEvents.validVIPEvent)
  val rddManualOverrideEvent = createRDDfromJsonToIMS(IMSEvents.validManualOverrideEvent)
  val rddManualOverrideWrongEvent = createRDDfromJsonToIMS(IMSEvents.invalidManualOverrideEvent)
  val rddManualOverrideEuportabilityFalseEvent = createRDDfromJsonToIMS(IMSEvents.manualOverrideEventEuportabilityFalse) //flag euportability false


  //filterInterestingEuPortabilityEvent
  "Manual Override Event type" when {
    "is equal to PROFILE_MODIFIED_EUP" should {
      "be not empty" in {
        val rddEuPortabilityEvents = filterEuPortabilityEvents(rddManualOverrideEvent)
        assert(!rddEuPortabilityEvents.isEmpty)
      }
    }

    "is different from PROFILE_MODIFIED_EUP" should {
      "be empty" in {

        val rddEuPortabilityEvents = filterEuPortabilityEvents(rddVIPEvent)
        assert(rddEuPortabilityEvents.isEmpty)
      }
    }

    "is equal to PROFILE_MODIFIED_EUP but Flag euportability = false" should {
      "be empty" in
      {
        val rddEuPortabilityEvents = filterEuPortabilityEvents(rddManualOverrideEuportabilityFalseEvent)
        assert(rddEuPortabilityEvents.isEmpty)
      }


    }


  }


  "Validation event manual override" when {
    "is ok" should {
      "be not empty" in {
        val checkValidityMessagesRDD: RDD[(IMS_Event, String, (scala.List[String], Boolean))] =  validateEvents(rddManualOverrideEvent, Validator.validateEuPortabilityEvent)
        assert(checkValidityMessagesRDD.map(x => x._3._1.forall(x => x == null)).filter(_ => false).isEmpty() &&  checkValidityMessagesRDD.map(x => x._3._2).filter(_ => false).isEmpty())
      }
    }

    "is not ok" in {
      val checkValidityMessagesRDD: RDD[(IMS_Event, String, (scala.List[String], Boolean))] =  validateEvents(rddManualOverrideWrongEvent, Validator.validateEuPortabilityEvent)
      assert(checkValidityMessagesRDD.filter(x => x._3._1.contains(null)).isEmpty() &&  checkValidityMessagesRDD.filter(x => x._3._2).isEmpty())
    }
  }


  "valid activity rdd" when {
    "is ok" should {
      "be empty" in {
        val activityRDD: RDD[Option[Activity]] = IMSEvent2Activity(rddManualOverrideEvent)
        val IMSEvent = rddManualOverrideEvent.first()._1
        val activity = activityRDD.first()

        assert(activity.nonEmpty)
        assert(IMSEvent.data.profile.profileid.toLowerCase() == activity.get.profile_id.toLowerCase() && activity.get.home_country.toLowerCase() == IMSEvent.providerTerritory.toLowerCase() && activity.get.provider.toLowerCase() == IMSEvent.provider.toLowerCase())
      }

    }
  }




  /*
        val rddExtracted = extractMessageRDD_ManualOverride(rdd, topic)
        val rddFiltered = excludeFailuresManualOverride(rddExtracted)
        val rddValidation = validationEventsManualOverride(rddFiltered).persist()
        val rddValidated = validatedEventsFilterManualOverride(rddValidation)
        val rddResult = filterEuPortabilityEvents(rddValidated)
        val model: IMS_Event = rddResult.first()

        "model filtered has to be correct" must {
          "field 1 has to be ...." in {pending}
          "fie
          ld 2 has to be ...." in {pending}
        }

      }
      "event is not in perimeter" must {
        val rdd = createRDDfromJSON(IMSEvents.eventNotInterestPerimeter._2)
        val rddExtracted = extractMessageRDD_ManualOverride(rdd, topic)
        val rddFiltered = excludeFailuresManualOverride(rddExtracted)
        val rddValidation = validationEventsManualOverride(rddFiltered).persist()
        val rddValidated = validatedEventsFilterManualOverride(rddValidation)
        val rddResult = filterEuPortabilityEvents(rddValidated)
        val model: IMS_Event = rddResult.first()

        "model filtered has to be correct" must {
          "field 1 has to be ...." in {pending}
          "field 2 has to be ...." in {pending}
        }
      }
  */


  //deletionSelectedActivityFromCassandra
  "CRUD to Activity table of Cassandra" ignore {
    import com.sky.dap.eu_portability.model.{Activity}
    val db = new CassandraTest

    val rdd: RDD[Activity] = sc.parallelize(Seq(Activity("999", "nowtv", "gb")))
    "insert into table" in {
      db.insertIntoCassandra(rdd, keyspace, tableName)
    }
    "delete from table" in {
      db.deleteFromCassandra(rdd, keyspace, tableName)
    }


  }

}
