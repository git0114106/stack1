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
object IPL_hatrick {
  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("IPL2").master("local[*]").config("spark.driver.host","localhost").getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("delimiter", ",").option("header", true).option("inferSchema", true).load("C:\\Users\\arul\\Dropbox\\data\\ipl\\deliveries.csv").toDF()
    df.printSchema()
   val df2=df.filter("match_id=37 ")
   // val match_503 = df.filter("match_id=37 ")
    //println("Number of balls played by match1 is " + match_503.count())
    // match_503.show(2,false)
    df2.createOrReplaceTempView("team")
      spark.sql(
        """
       with t1 (select over, ball, row_number() over(partition by inning order by  cast(concat(over,'.',ball) as double) ) ball2,
         player_dismissed, bowler, dismissal_kind, cast(concat(over,'.',ball) as double) over2

        from team --where player_dismissed is not null
       ),
       t2 (select over2, ball2, player_dismissed , dismissal_kind, bowler,
       case when  max(ball2) over(partition by bowler order by ball2 rows between current row and 2 following) - ball2 <3
        then 1 else 0 end rw1
        from t1 where player_dismissed is not null ),

        t3( select over2, ball2, player_dismissed , dismissal_kind, bowler, rw1,
        count(1) over(partition by bowler, rw1) rw2 from t2 )
        select over2, ball2, player_dismissed , dismissal_kind, bowler, rw1, rw2 from t3 where rw2=3 and rw1=1

        """.stripMargin).show(false)
    }

  }
