package json_play

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

object books_authors {

  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("ABC").master("local[*]").config("spark.driver.host","localhost").getOrCreate()

    import spark.implicits._
  // multiLineABC
    val df = spark.read.option("multiLine",true).option("mode","()ERMISSIVE").json("C:\\Users\\arul\\Dropbox\\data\\json\\language_learn_both.json")
    //val df = spark.read.option("mode","PERMISSIVE").json("C:\\Users\\arul\\Dropbox\\data\\json\\webrank.json")
    df.show(100,false) // webrank2.json
    df.printSchema()
    println(df.count)

  }
}
