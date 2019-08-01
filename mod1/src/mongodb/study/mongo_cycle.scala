package mongodb.study

import scala.math.BigInt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.bson.Document._

object mongo_cycle {
  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("mongodb intro").master("local[*]")
      .config("spark.driver.host","localhost")
      .config("spark.mongodb.input.uri","mongodb://127.0.0.1/IPL.matches")
      .config("spark.mongodb.output.uri","mongodb://127.0.0.1/IPL.matchx")
      .getOrCreate()

    import spark.implicits._

    val df = MongoSpark.load(spark.sparkContext).toDF()
    println(df.count)
    df.show(false)
    //println(rdd.first.toJson)
  }

}
// C:\Intellij\Projects\mod1\src\cricket\IPL_hatrick2.scala
