package com.sky.dap



import org.apache.spark.sql._
import org.apache.log4j.LogManager
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf


/**
  * Created by dap on 2/23/18.
  */




object Prova {


  def main(args : Array[String]) {

    val log = LogManager.getLogger("SparkStructuredStreaming")




    /*

    df
    |subtable                    |    subtable_new                        |
    |subtable.col1|subtable.col2 |  subtable_new.col1 | subtable_new.col2 |


    root
     |-- subtable: struct (nullable = true)
     |    |-- col1: array (nullable = true)
     |    |    |-- element: integer (containsNull = false)
     |    |-- col2: string (nullable = true)
     |-- subtable_new: struct (nullable = true)
     |    |-- col1: array (nullable = true)
     |    |    |-- element: integer (containsNull = false)
     |    |-- col2: string (nullable = true)
     */



  }
}
