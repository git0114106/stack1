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
object IPLv1 {
  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Spark_Abinitio_case_study9").master("local[*]").getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv")
                .option("delimiter", ",")
                .option("header",true)
                .option("inferSchema",true).load("C:\\Users\\arul\\Dropbox\\data\\ipl\\deliveries.csv").toDF()
      //toDF("match_id","inning","batting_team","bowling_team","over","ball","batsman","non_striker","bowler","is_super_over","wide_runs","bye_runs","legbye_runs","noball_runs","penalty_runs","batsman_runs","extra_runs","total_runs","player_dismissed","dismissal_kind","fielder")

    df.printSchema()
    /*val df2 = df.withColumn("match_id",'match_id.cast("int"))
                .withColumn("inning",'inning.cast("int"))
                .withColumn("over",'over.cast("int"))
                .withColumn("ball",'ball.cast("int"))
                .withColumn("is_super_over",'is_super_over.cast("int"))
                .withColumn("wide_runs",'wide_runs.cast("int"))
                .withColumn("bye_runs",'bye_runs.cast("int"))
                .withColumn("legbye_runs",'legbye_runs.cast("int"))
                .withColumn("noball_runs",'noball_runs.cast("int"))
                .withColumn("penalty_runs",'penalty_runs.cast("int"))
                .withColumn("batsman_runs",'batsman_runs.cast("int"))
                .withColumn("extra_runs",'extra_runs.cast("int"))
                .withColumn("total_runs",'total_runs.cast("int"))*/

    val rcb_hyd_503 = df.filter("match_id=503")
    println("Number of balls played by match1 is " + rcb_hyd_503.count() )
    rcb_hyd_503.show(false)
    // Scorecard
    val rcb_scorecard = rcb_hyd_503.filter("batting_team = 'Royal Challengers Bangalore'")
        .withColumn("over2",concat('over,'ball).cast("int"))
      .groupBy("batsman")
      .agg(
         min('over).as("over"),
         min('ball).as("ball"),
         min('over2).as("over2"),
        // concat(max('player_dismissed),lit(" "),max('dismissal_kind)).as("how_out1"),
         when(max('player_dismissed)==='batsman,concat(max('dismissal_kind),lit(" "),coalesce(max('fielder),max('bowler)))).as("how_out2"),
         sum('batsman_runs).as("R"),
         count(lit(1)).as("B"),
         sum(when('batsman_runs===lit(4),lit(1)).otherwise(0)).as("4's"),
         sum(expr("case when batsman_runs=6 then 1 else 0 end")).as("6's"),
         ((sum('batsman_runs)/count(lit(1))) * 100).cast(DecimalType(7,2)) as "S/R"
      )
      //.orderBy('over,'ball).show(false)
      .orderBy('over2).show(false)

    val teams = rcb_hyd_503.select('batting_team).groupBy().agg( collect_set('batting_team) as "teams").select('teams).as[Seq[String]].first
    println(teams)

    for( team <- teams ) { // List("Royal Challengers Bangalore","Sunrisers Hyderabad")) {
      val rcb_scorecard2 = rcb_hyd_503.filter(s"batting_team = '${team}'")
        .withColumn("over2", concat('over, 'ball).cast("int"))

      rcb_scorecard2.createOrReplaceTempView("rcb")
      val rcb_scorecard3 = spark.sql(
        """
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
         from t1 )
          select batsman, non_striker, player_dismissed, how_out,
          case when batsman=player_dismissed then -1
          when non_striker=player_dismissed then -1
          else count(1) over(partition by batsman) end as how_outx,
          R, B, `4s`, `6s`, `S/R` from t2
          order by ov2
      """.stripMargin)
      rcb_scorecard3.createOrReplaceTempView("rcb2")
      spark.sql(
        """
          select 'x' x, batsman, non_striker, player_dismissed, how_out R, B, `4s`, `6s`, `S/R` from rcb2 where batsman=player_dismissed
          union
          select 'y' x, batsman, non_striker, player_dismissed, how_out R, B, `4s`, `6s`, `S/R` from rcb2
          where batsman not in (select batsman from rcb2 where batsman=player_dismissed)
        """.stripMargin).show(false)

    }
// where how_out <> 'none'
    //rcb_hyd_503.filter("batting_team = 'Sunrisers Hyderabad'").groupBy("batsman").agg( sum('batsman_runs).as("batsman")).show(false)

  }
}
