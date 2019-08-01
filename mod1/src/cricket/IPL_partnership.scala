package cricket
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
object IPL_partnership {
  def main(argsi: Array[String]) {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder().appName("IPL").master("local[*]").getOrCreate()

      import spark.implicits._

      val df = spark.read.format("csv").option("delimiter", ",").option("header", true).option("inferSchema", true).load("C:\\Users\\arul\\Dropbox\\data\\ipl\\deliveries.csv").toDF()
      df.printSchema()

      val match_503 = df.filter("match_id=503")
      println("Number of balls played by match1 is " + match_503.count())
     // match_503.show(2,false)

      val teams = match_503.select('batting_team).groupBy().agg(collect_set('batting_team) as "teams").select('teams).as[Seq[String]].first
      println(teams)
    val partner_columns =List("batsman","non_striker","total_runs","total_runs","over2")
     for( team <- teams ) {
      val bat1 = match_503.filter(s"batting_team = '${team}'").withColumn("over2", concat('over, lit("."),'ball).cast("double"))
      // bat1.show(false)
       val bat2 = bat1.select(partner_columns.map(col(_)):_*)
       bat2.printSchema()
       val bat3=bat2.groupBy('batsman,'non_striker).agg(sum('total_runs).as("total_runs"),max('over2).as("over2"))

       //bat3.orderBy('over2).show(false)
       val bat4=bat3.alias("t1").join(bat3.alias("t2"), $"t1.batsman" === $"t2.non_striker" and $"t2.batsman" === $"t1.non_striker", "inner")
                  .withColumn("runs2", $"t1.total_runs" + $"t2.total_runs")
                  .withColumn("max_over",greatest($"t1.over2",$"t2.over2"))
                  .withColumn("player1", expr("case when t1.over2=max_over then t1.batsman else t2.batsman end"))
                  .withColumn("player2", expr("case when t1.over2=max_over then t1.non_striker else t2.non_striker end"))
       val bat5 = bat4.dropDuplicates(Array("player1","player2","max_over","runs2"))
                      .select("player1","player2","max_over","runs2")
       //bat5.orderBy($"max_over")show(false)
       val dismissed = bat1.filter(" length(player_dismissed) > 1").select("player_dismissed","over2")
       val bat6 = bat5.join(dismissed, bat5("max_over")===dismissed("over2") , "leftOuter" )
                        .withColumn("batsout",expr("case when player_dismissed=player1 then player1 else 'not out' end"))
       val bat7 = bat6.orderBy('max_over).select($"player1",$"player2",$"batsout",$"runs2".as("partnership_runs"))
       val bat8 = bat7.union(bat7.select(lit(" "),lit(" "),lit("Total"), sum($"partnership_runs")))
       bat8.show(false)
    }

  }
}
