package com.sky.dap.eu_portability.unit.vip

import com.sky.dap.eu_portability.AppMethods._
import com.sky.dap.eu_portability.{IdentityListener, UnitSpec}
import com.sky.dap.eu_portability.model.{IMS_Event, Vip}
import com.sky.dap.eu_portability.service.ModelConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by dap on 4/12/18.
  */
class IMSEvent2VIPUnitTest extends  UnitSpec("UnitTest")  {

  val spark: SparkSession = SparkSession
    .builder
    .appName(SPARK_APP_NAME).master(SPARK_MASTER)
    .getOrCreate()

  implicit val sc = spark.sparkContext
  implicit val conf = sc.getConf


  "transform valid IMSEvent to VIP" when {

    "IMS event with nowTv service, gb and globalAccess not null" when {

      val keyJson = "999"
      val json ="{ \"data\": { \"profile\": { \"contactemail\": \"shiva04041@sky.uk\", \"firstname\": \"ads\", \"mobilenumber\": null, \"targetedoptoutdate\": null, \"hhuserauthorised\": null, \"hhusertype\": \"guest\", \"nsprofileid\": \"e769d7b2-43c2-4192-a354-ec733a479ef8\", \"registrationdate\": 1522857559974, \"title\": \"Mr\", \"targetedoptin\": true, \"passwordrequireschange\": false, \"profileid\": \"999725354234\", \"email\": \"shiva04041@sky.uk\", \"targetedoptindate\": 1522857559974, \"dateofbirth\": null, \"euportability\": true, \"mobilenumberverified\": null, \"services\": { \"nowtv\": { \"mailbox\": null, \"name\": \"nowtv\", \"start\": 978307200000, \"end\": null, \"fullysignedup\": null, \"globalaccess\": 1539129600000, \"suspended\": false } }, \"hhid\": null, \"termsandconditionsaccepted\": true, \"mobilenumberchanged\": null, \"lastname\": \"asdf\", \"displayname\": null, \"securityquestionid\": null, \"skyoptin\": true, \"emailchanged\": 1522857559974, \"username\": \"ADSASDF\", \"emailverified\": false, \"trackingid\": \"ihFE+auWbWwmNpEKwDaCHNEdU1GDzLJGU3NUv4uhANQ=\" }, \"changes\": [ \"services/nowtv\" ] }, \"providerTerritory\": \"gb\", \"origin\": \"oogway-intl\", \"type\": \"eu_portability_input\", \"tags\": [ \"_dateparsefailure\" ], \"householdId\": \"999725354234\", \"@timestamp\": \"2018-04-04T16:30:11.625Z\", \"provider\": \"SKY\", \"profileid\": \"999725354234\", \"originatingSystem\": \"IMS\", \"@version\": \"1\", \"activityTimestamp\": \"2018-04-04T16:30:10.605Z\", \"id\": \"f117caf8-8291-42ca-b7fb-39c383b30765\", \"event\": \"PROFILE_MODIFIED_GLOBAL_ACCESS\", \"activityType\": \"profile_modified\" }"
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      "Vip returned must be equal in all interesting fields" must {

        val rddextracted: RDD[(Try[IMS_Event], String)] = extractMessageRDD(rdd)
        val rddexcludeFailure: RDD[(IMS_Event, String)] = excludeFailures(rddextracted)
        val VIP: Option[Vip] = ModelConverter.IMSEvent2Vip(rddexcludeFailure.first()._1)

        "is success" in {
          VIP.isEmpty shouldBe false

        }
        "provider Territory is equal" in {
        VIP.get.home_country shouldEqual "gb"
      }
        "provider is equal" in {
          VIP.get.provider shouldEqual "sky"
        }

        "profile id is equal" in {
          VIP.get.profile_id shouldEqual "999725354234"
        }
        "end_date is equal to global access" in {
          VIP.get.end_date shouldEqual 1539129600000L
        }
      }
    }

    "IMS event with nowTv service, gb and globalAccess null" when {

      val keyJson = "999"
      val json ="{\"data\": { \"profile\": { \"contactemail\": \"shiva04041@sky.uk\", \"firstname\": \"ads\", \"mobilenumber\": null, \"targetedoptoutdate\": null, \"hhuserauthorised\": null, \"hhusertype\": \"guest\", \"nsprofileid\": \"e769d7b2-43c2-4192-a354-ec733a479ef8\", \"registrationdate\": 1522857559974, \"title\": \"Mr\", \"targetedoptin\": true, \"passwordrequireschange\": false, \"profileid\": \"999725354235\", \"email\": \"shiva04041@sky.uk\", \"targetedoptindate\": 1522857559974, \"dateofbirth\": null, \"euportability\": true, \"mobilenumberverified\": null, \"services\": { \"nowtv\": { \"mailbox\": null, \"name\": \"nowtv\", \"start\": 978307200000, \"end\": null, \"fullysignedup\": null, \"globalaccess\": null, \"suspended\": false }, \"skygo\": { \"mailbox\": null, \"name\": \"skygo\", \"start\": 978307200000, \"end\": null, \"fullysignedup\": null, \"globalaccess\": 1539129600000, \"suspended\": false } }, \"hhid\": null, \"termsandconditionsaccepted\": true, \"mobilenumberchanged\": null, \"lastname\": \"asdf\", \"displayname\": null, \"securityquestionid\": null, \"skyoptin\": true, \"emailchanged\": 1522857559974, \"username\": \"ADSASDF\", \"emailverified\": false, \"trackingid\": \"ihFE+auWbWwmNpEKwDaCHNEdU1GDzLJGU3NUv4uhANQ=\" }, \"changes\": [ \"services/nowtv\" ] }, \"providerTerritory\": \"gb\", \"origin\": \"oogway-intl\", \"type\": \"eu_portability_input\", \"tags\": [ \"_dateparsefailure\" ], \"householdId\": \"999725354234\", \"@timestamp\": \"2018-04-05T09:58:24.621Z\", \"provider\": \"nowtv\", \"profileid\": \"999725354235\", \"originatingSystem\": \"IMS\", \"@version\": \"1\", \"activityTimestamp\": \"2018-04-05T09:58:24.000Z\", \"id\": \"a74d11fa-3ea3-4234-ab37-2b4e89b6072d\", \"event\": \"PROFILE_MODIFIED_GLOBAL_ACCESS\", \"activityType\": \"profile_modified\" }"
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      "Vip returned must be equal in all interesting fields" must {

        val rddextracted: RDD[(Try[IMS_Event], String)] = extractMessageRDD(rdd)
        val rddexcludeFailure: RDD[(IMS_Event, String)] = excludeFailures(rddextracted)
        val VIP: Option[Vip] = ModelConverter.IMSEvent2Vip(rddexcludeFailure.first()._1)

        "is success" in {
          VIP.isEmpty shouldBe false

        }
        "provider Territory is equal" in {
          VIP.get.home_country shouldEqual "gb"
        }
        "provider is equal" in {
          VIP.get.provider shouldEqual "nowtv"
        }
        "profile id is equal" in {
          VIP.get.profile_id shouldEqual "999725354235"
        }
        "end_date is equal to global access" in {
          VIP.get.end_date shouldEqual 9999999999999L
        }
      }
    }

    "IMS event with skygo service, gb and globalAccess 1539129611000" when {

      val keyJson = "999"
      val json ="{ \"data\": { \"profile\": { \"contactemail\": \"shiva04041@sky.uk\", \"firstname\": \"ads\", \"mobilenumber\": null, \"targetedoptoutdate\": null, \"hhuserauthorised\": null, \"hhusertype\": \"guest\", \"nsprofileid\": \"e769d7b2-43c2-4192-a354-ec733a479ef8\", \"registrationdate\": 1522857559974, \"title\": \"Mr\", \"targetedoptin\": true, \"passwordrequireschange\": false, \"profileid\": \"999725354235\", \"email\": \"shiva04041@sky.uk\", \"targetedoptindate\": 1522857559974, \"dateofbirth\": null, \"euportability\": true, \"mobilenumberverified\": null, \"services\": { \"nowtv\": { \"mailbox\": null, \"name\": \"nowtv\", \"start\": 978307200000, \"end\": null, \"fullysignedup\": null, \"globalaccess\": null, \"suspended\": false }, \"skygo\": { \"mailbox\": null, \"name\": \"skygo\", \"start\": 978307200000, \"end\": null, \"fullysignedup\": null, \"globalaccess\":1539129611000, \"suspended\": false } }, \"hhid\": null, \"termsandconditionsaccepted\": true, \"mobilenumberchanged\": null, \"lastname\": \"asdf\", \"displayname\": null, \"securityquestionid\": null, \"skyoptin\": true, \"emailchanged\": 1522857559974, \"username\": \"ADSASDF\", \"emailverified\": false, \"trackingid\": \"ihFE+auWbWwmNpEKwDaCHNEdU1GDzLJGU3NUv4uhANQ=\" }, \"changes\": [ \"services/skygo\" ] }, \"providerTerritory\": \"gb\", \"origin\": \"oogway-intl\", \"type\": \"eu_portability_input\", \"tags\": [ \"_dateparsefailure\" ], \"householdId\": \"999725354234\", \"@timestamp\": \"2018-04-05T09:58:24.621Z\", \"provider\": \"nowtv\", \"profileid\": \"999725354235\", \"originatingSystem\": \"IMS\", \"@version\": \"1\", \"activityTimestamp\": \"2018-04-05T09:58:24.000Z\", \"id\": \"a74d11fa-3ea3-4234-ab37-2b4e89b6072d\", \"event\": \"PROFILE_MODIFIED_GLOBAL_ACCESS\", \"activityType\": \"profile_modified\" }"
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      "Vip returned must be equal in all interesting fields" must {

        val rddextracted: RDD[(Try[IMS_Event], String)] = extractMessageRDD(rdd)
        val rddexcludeFailure: RDD[(IMS_Event, String)] = excludeFailures(rddextracted)
        val VIP: Option[Vip] = ModelConverter.IMSEvent2Vip(rddexcludeFailure.first()._1)

        "is success" in {
          VIP.isEmpty shouldBe false

        }
        "provider Territory is equal" in {
          VIP.get.home_country shouldEqual "gb"
        }
        "provider is equal" in {
          VIP.get.provider shouldEqual "nowtv"
        }
        "profile id is equal" in {
          VIP.get.profile_id shouldEqual "999725354235"
        }
        "end_date is equal to global access" in {
          VIP.get.end_date shouldEqual 1539129611000L
        }
      }
    }

    "IMS event with nowtv service, ie and globalAccess 1524096000000" when {

      val keyJson = "999"
      val json ="{\"data\": { \"profile\": { \"contactemail\": \"ui9milpo@example.com\", \"firstname\": \"Badger\", \"mobilenumber\": null, \"targetedoptoutdate\": null, \"hhuserauthorised\": true, \"hhusertype\": \"primary\", \"nsprofileid\": \"0e72335e-a0a3-42e9-8e26-de0c1d9bee2b\", \"registrationdate\": 1524062351990, \"title\": \"Mr\", \"targetedoptin\": true, \"passwordrequireschange\": false, \"profileid\": \"21942114\", \"email\": \"ui9milpo@example.com\", \"targetedoptindate\": 1524062351990, \"dateofbirth\": null, \"euportability\": true, \"mobilenumberverified\": null, \"services\": { \"nowtv\": { \"mailbox\": null, \"name\": \"nowtv\", \"start\": 1420070400000, \"end\": null, \"fullysignedup\": null, \"globalaccess\": 1524096000000, \"suspended\": false } }, \"hhid\": \"1676579623\", \"termsandconditionsaccepted\": true, \"mobilenumberchanged\": null, \"lastname\": \"Mole\", \"displayname\": null, \"securityquestionid\": null, \"skyoptin\": true, \"emailchanged\": 1524062351990, \"username\": \"TEST_GQTDBJU\", \"emailverified\": false, \"trackingid\": \"zfWclA3oqvapHrjHd4C/zMUdaS0lAO0wnJ70pfI5bqI=\" }, \"changes\": [ \"services/nowtv\" ] }, \"providerTerritory\": \"ie\", \"origin\": \"oogway-intl\", \"type\": \"eu_portability_input\", \"tags\": [ \"_dateparsefailure\" ], \"householdId\": \"21942114\", \"@timestamp\": \"2018-04-18T14:39:12.746Z\", \"provider\": \"NOWTV\", \"profileid\": \"21942114\", \"originatingSystem\": \"IMS\", \"@version\": \"1\", \"activityTimestamp\": \"2018-04-18T14:39:12.387Z\", \"id\": \"b599a35c-32da-4ee6-b10d-52b4ca1d2314\", \"event\": \"PROFILE_MODIFIED_GLOBAL_ACCESS\", \"activityType\": \"profile_modified\" }"
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      "Vip returned must be equal in all interesting fields" must {

        val rddextracted: RDD[(Try[IMS_Event], String)] = extractMessageRDD(rdd)
        val rddexcludeFailure: RDD[(IMS_Event, String)] = excludeFailures(rddextracted)
        val VIP: Option[Vip] = ModelConverter.IMSEvent2Vip(rddexcludeFailure.first()._1)


        "is success" in {
          VIP.isEmpty shouldBe true

        }
      }
    }




  }
}
