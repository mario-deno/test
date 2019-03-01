import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Dataset

object test {

  def main(args: Array[String]): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("myapp")
      .getOrCreate



    val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    //val df = spark.read.format("csv").option("header", "false").option("delimiter", "::").load("hdfs:///raw/linkedin.csv")

    val source= Seq(
      ("batà::ciao,cie"),
      ("mousà::perse"),
      ("horsà::foer::ree")
    ).toDF("value")


    val b= Seq(
      ("mousà"),
      ("horsà")
    ).toDF("v")

    //val df1 = source.rdd.map(i => i.mkString.split("::").mkString(",")).toDF()
    //val iterationColumnLength = df1.rdd.first.mkString("::").split(",").length
    val df = source.withColumn("value2",split(col("value"),"::")).select((0 until 3).map(i => col("value2").getItem(i).as("col_" + i)): _*)//.show(false)

    df.join(b,df("col_0") === b("v"), "left_outer").show(false)


  }




}
