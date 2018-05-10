package com.sky.dap.eu_portability.integration

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.sky.dap.eu_portability.{SparkFactory, UnitSpec}
import com.sky.dap.eu_portability.SparkFactory._
import com.sky.dap.eu_portability.model.{Activity, IMS_Event}
import com.sky.dap.eu_portability.unit.manual_override.CassandraTest
import com.sky.dap.eu_portability.data.IMSEvents
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class E2E_ManualOverrideTest extends  UnitSpec("EndToEndTest")  {

  // TODO : Externalize parameters
  // ************************************
  val env = "dev"
  val cassandraHost ="localhost"
  val cassandraPort = "9042"
  // ************************************

  getSparkWithCassandra(SPARK_APP_NAME, SPARK_MASTER, cassandraHost, cassandraPort)

  val keyspace = "euportability_" + env
  val tableName = "activity"

  implicit val sc = spark.sparkContext
  val connector = CassandraConnector(sc.getConf)
  val db = new CassandraTest

  {
    require(db.checkKeyspace(sc,keyspace) == 1 && db.checkTable(sc, keyspace, tableName) == 1 )
    Given("Send Correct Event")
    // Generate event method
    val event = IMSEvents.validManualOverrideEvent
    When("user is present in Cassandra")
    // user is present, insert User in Cassandra
      val rdd : RDD[Activity] = sc.parallelize(Seq(Activity("999", "nowtv","gb")))
      db.insertIntoCassandra(rdd,keyspace, tableName)

    Then("record will be removed")
    // Assertion Record not found
      val activity = workflow(event)
      db.searchProfileIdIntoCassandra(sc, keyspace,tableName)("999")




      activity.isEmpty shouldBe true
  }
  {
    Given("Send Correct Event")
    // Generate event method
    val event = IMSEvents.validManualOverrideEvent
    When("user is not present in Cassandra")
    // user is not present, no action in Cassandra
    Then("no action maked")
    // Assertion Record not found
    val activity = workflow(event)
    activity.isEmpty shouldBe true
  }
  {
    Given("Send Event not in perimeter")
    // Generate event method
    When("user is present in Cassandra")
    // user is present in Activity
    Then("no action maked")
    // Assertion Record found and no action maked

  }
  {
    Given("Send Event not compatible with model")
    // Generate event method
    When("user is present in Cassandra")
    // user is present in Activity
    Then("no action maked")
    // Assertion Record found and no action maked

  }




  def workflow(event : String): Option[Activity] =
  {
    //load config
    //define perimeter --> markUsefulEventsRDD
    //exclude not in perimeter --> excludeNotUsefulEvent
    //deserialize message (extraction with model) --> extractMessageRDD/ extractMessageRDD_ManualOverride
    //exclude failures --> excludeFailures/ excludeFailuresManualOverride


    // VIP : filterVipEvents
    //computeVIP --> computeManualOverride

    //validation filter --> validateEvents/ validationEventsManualOverride + validatedEventsFilterManualOverride
    // MOv : filterInterestingEuPortabilityEvent
    // VIP : transformation from Event to VIP --> IMSEvent2VIP

    // VIP : insert into cassandra --> saveVipToCassandra
    // MOv : deletionSelectedActivityFromCassandra
    None
  }

}
