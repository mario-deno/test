package com.sky.dap.eu_portability

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.SparkSession

object SparkFactory {
  var spark:SparkSession = null

  private def init(appName: String, master: String) = {
    val session = SparkSession
      .builder
      .appName(appName)
      .master(master)

    spark = session.getOrCreate()
    spark
  }

  private def initWithCassandra(appName: String, master: String, cassandraHost : String, cassandraPort : String) = {
    val session = SparkSession
      .builder
      .appName(appName).master(master)
      .config(CassandraConnectorConf.ConnectionHostParam.name, cassandraHost)
      .config(CassandraConnectorConf.ConnectionPortParam.name, cassandraPort)

    spark = session.getOrCreate()
    spark
  }

  def getSpark(appName: String, master: String) = if (spark == null) init(appName, master) else spark
  def getSparkWithCassandra(appName: String, master: String, cassandraHost : String, cassandraPort : String) =
    if (spark == null) initWithCassandra(appName, master, cassandraHost, cassandraPort) else spark
  def close() = if (spark != null) spark.stop()
}
