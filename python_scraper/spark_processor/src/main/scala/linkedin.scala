import org.apache.spark.sql.functions._
import scala.util.matching.Regex


object linkedin {
  def main(args: Array[String]) {


    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("myapp")
      .getOrCreate


    val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    //val df = spark.read.format("csv").option("header", "false").option("delimiter", "::").load("hdfs:///raw/linkedin.csv")
    val source  = spark.read.option("header", "false").textFile("/home/dap/github/python_scraper/linkedin_machine_learning.txt")
    //capitalize column 1 (company name) string
    val company_name_dataset = spark.read.format("csv").option("header", "true").option("encoding", "UTF-8").load("/home/dap/github/multiclass_classification/data_source/company_name.csv").map(x => (normalizeString(x.getString(0)),x.getString(1)))


    //val df1 = source.rdd.map(i => i.mkString.split("::").mkString(",")).toDF()
    val iterationColumnLength = 6//df1.rdd.first.mkString(",").split(",").length
    //val df = df1.withColumn("value",split(col("value"),",")).select((0 until iterationColumnLength).map(i => col("value").getItem(i).as("col_" + i)): _*)
    val df = source.withColumn("value",split(col("value"),"::")).select((0 until iterationColumnLength).map(i => col("value").getItem(i).as("col_" + i)): _*)

    println("source count: " + df.count())

    //filter only on real names (not me)
    val df2 = df.filter("col_0 != 'LinkedIn Member' AND col_1 not like '%You'")

    //register udf to extract company name
    val udfCompanyName = udf(getCompanyName)
    // extract company name
    val df3 = df2.withColumn("company_Name",udfCompanyName(col("col_5"),col("col_3")))
    // rename column
    val df4 = df3.withColumnRenamed("col_0", "name").withColumnRenamed("col_3", "roleDescription").select("name","roleDescription","company_Name")


    //join with company dataset to standardize company name
    val df5 = df4.join(company_name_dataset, df4("company_Name")===company_name_dataset("_1"), "left_outer")

    //poject useful columns
    val result = df5.select("name","roleDescription","_2").withColumnRenamed("_2","company")

    println("output count: " + result.count())

    result.printSchema()


    /*val dfs = df3.filter("col_0 == 'Adriana Santacroce'")*/

    //save dataframe

    result
          .repartition(1)
          .write.format("csv")
          .option("header", "true")
          .option("encoding", "UTF-8")
          .save("output.csv")


  }

  def getCompanyName : ((String, String) => String) = (col1:String, col2:String) => {

    //val pattern = new Regex("(at\\s|presso\\s)(.*$)") //400
    //val pattern = new Regex("(at\\s|presso\\s)(\\w+ \\w-\\w+|[A-Za-z0-9. &àèìòù]*$|\\w+-[A-Za-z0-9. &àèìòù]*|[àèìòù\\w+\\s]{1,})")
    val pattern = new Regex("(at\\s|presso\\s|\\@\\s{0,1})(\\w+ \\w-\\w+|[A-Za-z0-9. &àèìòù]*$|\\w+-[A-Za-z0-9. &àèìòù]*|[àèìòù\\w+\\s]{1,})")

    //val pattern = new Regex("(at\\s|presso\\s)(\\w+ \\w-\\w+|[A-Za-z0-9. &àèìòù]*$|\\w+-[A-Za-z0-9. &àèìòù]*|[àèìòù\\w+\\s.]{1,})")
    var a,b: scala.Iterator[Regex.Match] = scala.Iterator.empty

    if (col1 != null) { a = pattern.findAllIn(col1).matchData }
    if (col2 != null) { b = pattern.findAllIn(col2).matchData }

    if (a.hasNext) { normalizeString(a.next().group(2)) }
    else { if (b.hasNext) { normalizeString(b.next().group(2))}
      else {
          null
      }
    }
  }

  def normalizeString(x:String) : String  = {
    x.replace("à","a").trim().replace("è","e").replace("ì","i").replace("ò","o").replace("ù","u").toLowerCase

  }

}