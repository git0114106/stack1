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
// This code prints the not-out batsman from each teams.. and then the scorecard..
// not-out batsman has a corner case.. what if the batsman remained at the non-striker end itself
object IPL_scorecard {
  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Spark IPL").master("local[*]").getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv")
      .option("delimiter", ",")
      .option("header",true)
      .option("inferSchema",true).load("C:\\Users\\arul\\Dropbox\\data\\ipl\\deliveries.csv").toDF()
     df.printSchema()

    val rcb_hyd_503 = df.filter("match_id=503")
    println("Number of balls played by match1 is " + rcb_hyd_503.count() )
    rcb_hyd_503.show(false)

    val teams = rcb_hyd_503.select('batting_team).groupBy().agg( collect_set('batting_team) as "teams").select('teams).as[Seq[String]].first
    println(teams)


    for( team <- teams ) {
      val rcb_scorecard2 = rcb_hyd_503.filter(s"batting_team = '${team}'").withColumn("over2", concat('over, 'ball).cast("int"))
      val bt_players = rcb_scorecard2.select('batsman as "a" ).dropDuplicates() //groupBy().agg( collect_set('batsman) as "batsman2", collect_set('player_dismissed) as "dismissed_p")
      val dismissed_players = rcb_scorecard2.select('player_dismissed as "a" ).where(" a is not null").dropDuplicates()
      //bt_players.show(false)
      //dismissed_players.show(false)
      bt_players.except(dismissed_players).show(false)
      val not_out_players = bt_players.except(dismissed_players).groupBy().agg(collect_set('a) as "not_out").select("not_out").as[Array[String]].first.mkString(",")
      println(not_out_players)

      rcb_scorecard2.createOrReplaceTempView("rcb")
      val rcb_scorecard3 = spark.sql(
        s"""
         with t1 ( select batsman, non_striker,player_dismissed,
         min(over2) over(partition by batsman ) ov2,
         sum(batsman_runs) over(partition by batsman) R,
         case when batsman=player_dismissed then dismissal_kind || ' '  || coalesce(concat(fielder, ' b ',bowler), bowler)
              when non_striker=player_dismissed then dismissal_kind || ' '  || coalesce(fielder,'--')
              else ' '
         end how_out,
         sum(case when wide_runs=1 then 0 else 1 end) over(partition by batsman) B,
         sum(case when batsman_runs=4 then 1 else 0 end) over(partition by batsman) `4s`,
         sum(case when batsman_runs=6 then 1 else 0 end) over(partition by batsman) `6s`
         from rcb ),

         t2 (select distinct batsman,non_striker,player_dismissed, ov2, how_out, R, B, `4s`, `6s`, cast(R/B*100 as decimal(7,2))as `S/R`
         from t1 ),
          t3( select split('${not_out_players}',',') x,
          batsman, non_striker, player_dismissed, how_out,
          case when array_contains(split('${not_out_players}',','),batsman) then 1 else 0 end as how_outx,
          R, B, `4s`, `6s`, `S/R`,ov2 from t2)
          select batsman, non_striker, player_dismissed, how_out, how_outx,
          R, B, `4s`, `6s`, `S/R` from t3 where how_outx=1 or batsman=player_dismissed or length(player_dismissed)>1
          or array_contains(split('${not_out_players}',','),non_striker)
          order by ov2
      """.stripMargin)
      rcb_scorecard3.show(false)

    }

  }
}
