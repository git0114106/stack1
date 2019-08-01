package stack
import scala.math.BigInt
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import scala.collection.mutable._
object crosstab {

  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Spark_Abinitio_case_study3").master("local[*]").getOrCreate()

    import spark.implicits._

    var someDF = Seq(
      (1, "2017-12-02 03:04:00"),
      (1, "2017-12-02 03:45:00"),
      (1, "2017-12-02 04:04:00"),
      (2, "2017-12-02 04:14:00"),
      (2, "2017-12-02 04:54:00"),
      (3, "2017-10-01 11:45:20"),
      (4, "2017-10-01 02:45:20")
    ).toDF("number", "date")

    someDF.show()
    println("""Here""")
    var temp = someDF.stat.crosstab("date", "number")
    temp.show(false)

    someDF.withColumn("datehour", substring($"date", 0, 13)).stat.crosstab("datehour", "number").show(false)
    println("with Pivot")
    someDF.withColumn("datehour", substring($"date", 0, 13)).groupBy("datehour").pivot("datehour").agg( count($"number"), sum('number) ).show(false)
    //someDF.stat.

  }
}
