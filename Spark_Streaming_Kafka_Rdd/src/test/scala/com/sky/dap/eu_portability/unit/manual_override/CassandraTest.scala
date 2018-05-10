package com.sky.dap.eu_portability.unit.manual_override

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.sky.dap.eu_portability.SparkFactory._
import com.sky.dap.eu_portability.UnitSpec
import com.sky.dap.eu_portability.model.Activity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class CassandraTest extends UnitSpec("CassandraTest") {
  //val loadConfig = LoadConfig.getConfig(env)

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


  "Cassandra environment" must {

    "keyspaceProva is not exists" in {
      val check = checkKeyspace(sc, "KeyspaceProva")
      check shouldBe 0
    }

    "keyspace Euportability is exists" in {
      val check = checkKeyspace(sc, keyspace)
      check shouldBe 1
    }

    "prova table not exists" in {
      val check = checkTable(sc, keyspace, "prova")
      check shouldBe 0
    }

    "activity table exists" in {
      val check = checkTable(sc, keyspace, "activity")
      check shouldBe 1
    }
  }


  def checkKeyspace(sc: SparkContext, keyspace: String): Long = {
    //select "keyspace_name" from system_schema.keyspaces where keyspace_name = 'euportability_dev';
    sc.cassandraTable("system_schema","keyspaces")
      .select("keyspace_name")
      .where("keyspace_name = ?", keyspace).collect().length
  }

    def checkTable(sc: SparkContext, keyspace: String, table: String): Long = {
      sc.cassandraTable("system_schema","tables")
        .select("table_name")
        .where("keyspace_name = ? AND table_name = ?", keyspace, table).collect().length
    }

    def countRowsTable(sc: SparkContext, keyspace: String, table: String): Long = {
      sc.cassandraTable(keyspace,table).select("profile_id").collect().length
    }

  def insertIntoCassandra(rdd: RDD[Activity], keyspace : String, tableName:String) (implicit sc: SparkContext) : Unit  =
  {
    rdd.saveToCassandra(keyspace, tableName)
  }

  def deleteFromCassandra(rdd: RDD[Activity], keyspace : String, tableName:String) (implicit sc: SparkContext) : Unit  =
  {
    rdd.deleteFromCassandra(keyspace, tableName)
  }

  def searchProfileIdIntoCassandra(sc: SparkContext, keyspace: String, table: String)(profile_id : String): Array[Activity] = {
    sc.cassandraTable[Activity](keyspace, table)
      .select("profile_id")
      .where("profile_id = ? ", profile_id).collect()
  }


}
