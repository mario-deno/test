package com.sky.dap.eu_portability.service




import com.datastax.spark.connector._
import com.sky.dap.eu_portability.model.{Activity, KafkaOffset, Vip}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._



object CassandraActivityService   {

  def deleteActivityToCassandra(activityConversionSuccessRDD: RDD[Activity], cassandraKeyspace: String, cassandraActivityTableName: String) = {
    activityConversionSuccessRDD.deleteFromCassandra(cassandraKeyspace, cassandraActivityTableName)
  }


  def saveVipToCassandra(rdd: RDD[Vip], keyspace : String, tableName:String ) (implicit sparkConf: SparkConf): Unit =
  {
    rdd.saveToCassandra(keyspace,tableName,SomeColumns("profile_id","home_country","provider","end_date","start_date"))
  }

  def readOffsetFromCassandra(keyspace : String, tableName:String) (implicit sc: SparkContext) : RDD[KafkaOffset]  =
  {
    val rdd = sc.cassandraTable[KafkaOffset](keyspace, tableName)
    rdd
  }


  def saveOffsetFromCassandra(rdd: RDD[KafkaOffset], keyspace : String, tableName:String) (implicit sc: SparkContext) : Unit  =
  {
    rdd.saveToCassandra(keyspace, tableName,SomeColumns("group_id","topic","partition","next_offset"))
  }



}
