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
object IPL_correlated_query {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("IPL2").master("local[*]").config("spark.driver.host","localhost").getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("delimiter", ",").option("header", true).option("inferSchema", true).load("C:\\Users\\arul\\Dropbox\\data\\ipl\\deliveries.csv").toDF()
    df.printSchema()
    val df2=df.filter("match_id=503 and inning=1  ") // or match_id=110  or match_id=19

    df2.createOrReplaceTempView("team")

    spark.sql(
      """
         -- Using Join for over snapshot of runs alone..
      with t1 ( select over over2, sum(total_runs) total_runs2,count(player_dismissed) wkt from team group by over )
      select x.over2, x.total_runs2, sum(y.total_runs2) cumulative_runs, sum(y.wkt) wkt2 from t1 x join t1 y
      on x.over2 >= y.over2
      group by x.over2, x.total_runs2
      order by x.over2
        """.stripMargin).show(300,false)
  }

}

/*
with t1 (select match_id, inning, cast(concat(over,'.',ball) as double) ball2, batsman, noball_runs,
       case when lag(noball_runs) over(partition by match_id, inning order by  cast(concat(over,'.',ball) as double) )>0
           then 1 else 0 end as freehit_ind,
       total_runs, total_runs-noball_runs as freehit_runs, bowler from team
       )
       select * from t1 where freehit_ind <> 0
       union
       select * from t1 where noball_runs =1
       order by match_id, inning, ball2

 */