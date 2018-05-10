package com.sky.dap.eu_portability.unit.vip

import com.sky.dap.eu_portability.AppMethods._
import com.sky.dap.eu_portability.UnitSpec
import com.sky.dap.eu_portability.model.IMS_Event
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by dap on 4/12/18.
  */
class MarshallingEventUnitTest extends  UnitSpec("UnitTest")  {

  val spark: SparkSession = SparkSession
    .builder
    .appName(SPARK_APP_NAME).master(SPARK_MASTER)
    .getOrCreate()

  implicit val sc = spark.sparkContext
  implicit val conf = sc.getConf




  "marshall IMSEvent" when {

    "json PROFILE_MODIFIED_EUP is compatible with the model" must {

      val keyJson = "999"
      val json ="{ \"activityTimestamp\":\"2018-02-15T16:29:52.039Z\", \"provider\":\"NOWTV\", \"event\":\"PROFILE_MODIFIED_EUP\", \"data\":{ \"profile\":{ \"services\":{ }, \"mobilenumber\":null, \"targetedoptin\":true, \"targetedoptoutdate\":null, \"email\":\"3iydhpyw@example.com\", \"passwordrequireschange\":false, \"targetedoptindate\":1518712191987, \"hhuserauthorised\":null, \"hhusertype\":\"primary\", \"username\":\"TEST_RIGKFNC\", \"skyoptin\":true, \"dateofbirth\":null, \"firstname\":\"Badger\", \"nsprofileid\":\"084e06cc-478f-4ca2-81c1-56bceb1d0a55\", \"hhid\":\"2297238189\", \"euportability\":true, \"emailchanged\":1518712191987, \"contactemail\":\"3iydhpyw@example.com\", \"lastname\":\"Mole\", \"mobilenumberverified\":null, \"displayname\":null, \"securityquestionid\":null, \"profileid\":\"20157336\", \"mobilenumberchanged\":null, \"emailverified\":false, \"trackingid\":\"6+jOd1EFdY0BVH8qcVk23GPVWcPp4wOXtKI36oViANI=\", \"title\":\"Mr\", \"registrationdate\":1518712191987, \"termsandconditionsaccepted\":true }, \"changes\":[ \"euportability\" ] }, \"id\":\"b5ea7dea-4afb-4ad1-bdf6-37ff9a6151a0\", \"providerTerritory\":\"IE\", \"origin\":\"oogway-intl\", \"profileid\":\"20157336\", \"originatingSystem\":\"IMS\", \"householdId\":\"20157336\", \"activityType\":\"PROFILE_MODIFIED\" }"
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      "first element of rdd must be success" in {
        val rddResult : RDD[(Try[IMS_Event], String)] = extractMessageRDD(rdd)
        val result = rddResult.first()

        result._1.isSuccess shouldBe true
      }
    }

    "json is not compatible with the model" must {

      val keyJson = "999"
      val json ="{\"name\":\"Pippo\"}"
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      "first element of rdd must be failure"  in {
        val rddResult : RDD[(Try[IMS_Event], String)] = extractMessageRDD(rdd)
        val result = rddResult.first()

        result._1.isSuccess shouldBe false
      }
    }


    "json PROFILE_MODIFIED_GLOBAL_ACCESS is compatible with the model" must {

      val keyJson = "999"
      val json ="{ \"data\": { \"profile\": { \"contactemail\": \"shiva04041@sky.uk\", \"firstname\": \"ads\", \"mobilenumber\": null, \"targetedoptoutdate\": null, \"hhuserauthorised\": null, \"hhusertype\": \"guest\", \"nsprofileid\": \"e769d7b2-43c2-4192-a354-ec733a479ef8\", \"registrationdate\": 1522857559974, \"title\": \"Mr\", \"targetedoptin\": true, \"passwordrequireschange\": false, \"profileid\": \"999725354234\", \"email\": \"shiva04041@sky.uk\", \"targetedoptindate\": 1522857559974, \"dateofbirth\": null, \"euportability\": true, \"mobilenumberverified\": null, \"services\": { \"nowtv\": { \"mailbox\": null, \"name\": \"nowtv\", \"start\": 978307200000, \"end\": null, \"fullysignedup\": null, \"globalaccess\": 1539129600000, \"suspended\": false } }, \"hhid\": null, \"termsandconditionsaccepted\": true, \"mobilenumberchanged\": null, \"lastname\": \"asdf\", \"displayname\": null, \"securityquestionid\": null, \"skyoptin\": true, \"emailchanged\": 1522857559974, \"username\": \"ADSASDF\", \"emailverified\": false, \"trackingid\": \"ihFE+auWbWwmNpEKwDaCHNEdU1GDzLJGU3NUv4uhANQ=\" }, \"changes\": [ \"services/nowtv\" ] }, \"providerTerritory\": \"gb\", \"origin\": \"oogway-intl\", \"type\": \"eu_portability_input\", \"tags\": [ \"_dateparsefailure\" ], \"householdId\": \"999725354234\", \"@timestamp\": \"2018-04-04T16:30:11.625Z\", \"provider\": \"SKY\", \"profileid\": \"999725354234\", \"originatingSystem\": \"IMS\", \"@version\": \"1\", \"activityTimestamp\": \"2018-04-04T16:30:10.605Z\", \"id\": \"f117caf8-8291-42ca-b7fb-39c383b30765\", \"event\": \"PROFILE_MODIFIED_GLOBAL_ACCESS\", \"activityType\": \"profile_modified\" }"
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      "first element of rdd must be success"  in {
        val rddResult : RDD[(Try[IMS_Event], String)] = extractMessageRDD(rdd)
        val result = rddResult.first()

        result._1.isSuccess shouldBe true
      }
    }


  }
}
