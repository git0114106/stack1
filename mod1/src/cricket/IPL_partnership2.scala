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
object IPL_partnership2 {
  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("IPL").master("local[*]").getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("delimiter", ",").option("header", true).option("inferSchema", true).load("C:\\Users\\arul\\Dropbox\\data\\ipl\\deliveries.csv").toDF()
    df.printSchema()

    val match_503 = df.filter("match_id=504")
    println("Number of balls played by match1 is " + match_503.count())
    // match_503.show(2,false)

    val teams = match_503.select('batting_team).groupBy().agg(collect_set('batting_team) as "teams").select('teams).as[Seq[String]].first
    println(teams)
    val partner_columns =List("batsman","non_striker","total_runs","total_runs","over2")
    for( team <- teams ) {
      val bat1 = match_503.filter(s"batting_team = '${team}'").withColumn("over2", concat('over, lit("."),'ball).cast("double"))
      // bat1.show(false)
     bat1.createOrReplaceTempView("team")
      spark.sql(
        """
          with t1 ( select batsman, non_striker, over2, player_dismissed, sum(total_runs) over(order by over2) pt_runs1 from team ),
          t2 ( select * from t1 where over2=(select max(over2) from team ) ),
          t3 ( select batsman, non_striker, player_dismissed, over2, pt_runs1 from t1 where length(player_dismissed)> 1
          union
          select batsman, non_striker, player_dismissed, over2, pt_runs1 from t2 )
          select '1' a, batsman, non_striker, coalesce(player_dismissed,'not out') player_dismissed,   pt_runs1, coalesce(pt_runs1-lag(pt_runs1) over(order by over2),pt_runs1) pt_runs2 from t3
          union
          select '2' a, ' ' , ' ', 'Total' , null , max(pt_runs1) from t3
          order by a, coalesce(pt_runs1,9999)
        """.stripMargin).show(false)
    }

  }
}
