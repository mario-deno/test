package com.sky.dap.eu_portability.unit.vip

import com.sky.dap.eu_portability.UnitSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.sky.dap.eu_portability.AppMethods._

/**
  * Created by dap on 4/12/18.
  */
class UnitTest extends  UnitSpec("UnitTest")  {

  val spark: SparkSession = SparkSession
    .builder
    .appName(SPARK_APP_NAME).master(SPARK_MASTER)
    .getOrCreate()

  implicit val sc = spark.sparkContext
  implicit val conf = sc.getConf




  "define perimeter method" when {

    "json has not event field" must {

      val keyJson = "999"
      val json ="{\"name\":\"Pippo\"}"
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      "result value is False" in {
        val rddResult : RDD[(String, String, Boolean)]= markUsefulEventsRDD(rdd)
        val result = rddResult.first()

        result._3 shouldBe false
      }
    }

    "json is empty" must {

      val keyJson = "999"
      val json =""
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      val rddResult : RDD[(String, String, Boolean)]= markUsefulEventsRDD(rdd)
      val result = rddResult.first()

      "boolean value is False" in {

        result._3 shouldBe false
      }
    }


    "json has event field" when {
      val keyJson = "999"
      val json ="{\"event\":\"NOT_IN_PERIMETER\"}"
      val arrayJson = Array( (keyJson,json) )
      val rdd = sc.parallelize(arrayJson)

      val rddResult : RDD[(String, String, Boolean)]= markUsefulEventsRDD(rdd)
      val result = rddResult.first()

      "boolean value is False" in {

        result._3 shouldBe false
      }



      "and is in perimeter with PROFILE_MODIFIED_EUP" in {
        val json ="{\"event\":\"PROFILE_MODIFIED_EUP\"}"
        val arrayJson = Array( (keyJson,json) )
        val rdd = sc.parallelize(arrayJson)

        val rddResult : RDD[(String, String, Boolean)]= markUsefulEventsRDD(rdd)
        val result = rddResult.first()

        result._3 shouldBe true

      }
      "and is in perimeter with PROFILE_MODIFIED_GLOBAL_ACCESS " in {
        val json ="{\"event\":\"PROFILE_MODIFIED_GLOBAL_ACCESS\"}"
        val arrayJson = Array( (keyJson,json) )
        val rdd = sc.parallelize(arrayJson)

        val rddResult : RDD[(String, String, Boolean)]= markUsefulEventsRDD(rdd)
        val result = rddResult.first()

        result._3 shouldBe true
      }
    }
  }

  "exclude non interesting event" when {

    "rdd passed in input has the first field of 3rd couple equals to false" must {
      "rdd resulting is empty" in {

        val keyJson = "999"
        val json ="{\"name\":\"Pippo\"}"
        val interesting = false
        val array = Array((keyJson,json,interesting))
        val rdd = sc.parallelize(array)
        val rddResult = excludeNotUsefulEvent(rdd)

        rddResult.isEmpty() shouldBe  true
      }
    }

    "rdd passed in input has the first field of 3rd couple equals to true" must {
      "rdd resulting is not empty" in {
        val keyJson = "999"
        val json ="{\"name\":\"Pippo\"}"
        val interesting = true
        val eventTypeValue = "PROFILE_MODIFIED_GLOBAL_ACCESS"
        val array = Array((keyJson,json,interesting))
        val rdd = sc.parallelize(array)
        val rddResult = excludeNotUsefulEvent(rdd)

        rddResult.isEmpty() shouldBe  false

      }
    }
  }
}
