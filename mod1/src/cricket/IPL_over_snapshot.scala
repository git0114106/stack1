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
object IPL_over_snapshot {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("IPL2").master("local[*]").config("spark.driver.host","localhost").getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("delimiter", ",").option("header", true).option("inferSchema", true).load("C:\\Users\\arul\\Dropbox\\data\\ipl\\deliveries.csv").toDF()
    df.printSchema()
    val df2=df.filter("match_id=503")
    val teams = df2.select("batting_team").groupBy("batting_team").pivot("batting_team").count.columns.drop(1)
    println(teams.mkString)

    // Method1
    for( team <- teams)
      {
        df2.filter(s"batting_team='${team}'").createOrReplaceTempView("team")
        println(team.toUpperCase)
        spark.sql(
          """
            with t1 as ( select over, sum(total_runs) runs, sum(case when player_dismissed is null then 0 else 1 end) wkt from team group by over )
            select over, sum(runs) over(order by over) runs1, sum(wkt) over(order by over) wkt from t1 order by over
          """.stripMargin).show(false)
      }
    // Method2
    for( team <- teams)
    {
      df2.filter(s"batting_team='${team}'").createOrReplaceTempView("team")
      println(team.toUpperCase)
      spark.sql(
        """
            with t1( select over, sum(total_runs) over(order by over) runs, sum(case when player_dismissed is null then 0 else 1 end) over(order by over) wkt from team )
            select over, max(runs) runs1, max(wkt) wkt from t1 group by over
          """.stripMargin).show(false)
    }
  /*
)
    df2.createOrReplaceTempView("team")
    spark.sql(
      """
       with t1 (select over, inning, sum(total_runs) from team group by over
       )
       select * from t1 where freehit_ind <> 0
       union
       select * from t1 where noball_runs =1
       order by match_id, inning, ball2

        """.stripMargin).show(300,false)
        */
  }

}

/*
 */