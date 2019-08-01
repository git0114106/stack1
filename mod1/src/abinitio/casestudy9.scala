package abinitio
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
object casestudy9 {
  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Spark_Abinitio_case_study9").master("local[*]").getOrCreate()

    import spark.implicits._
    val df = spark.read.format("csv").option("delimiter", "~").load("mod1/abinitio/in/case9.txt").toDF("id", "dummy1", "event", "dummy2", "ts").drop("dummy1").drop("dummy2")
    val df2 = df.withColumn("ts", to_timestamp(concat(current_date, lit(" "), 'ts)))
    df2.createOrReplaceTempView("emp_login")

    df2.printSchema()
    df2.show(100, false)

    println(" Sequence Number matching: ")

    spark.sql(
      """
            with t1 as ( select id, event, ts, row_number() over(partition by id order by ts) as rw from emp_login),
            t2 as ( select t1.id, t1.event, t1.ts entry, tt.ts exiit ,
                  ((HOUR(tt.ts) - HOUR(t1.ts)) * 60*60)
                  + ((MINUTE(tt.ts) - MINUTE(t1.ts)) * 60)
                  +  (SECOND(tt.ts) - SECOND(t1.ts))
                as diff_seconds,
            t1.rw, tt.rw from t1 join t1 tt on t1.id=tt.id
            and t1.event ='ENTRY' and tt.event='EXIIT'
            and t1.rw+1=tt.rw
            )
            select * from t2
          """).show(100, false)


    println(" Results: ")
    spark.sql(
      """
            with t1 as ( select id, event, ts, row_number() over(partition by id order by ts) as rw from emp_login),
            t2 as ( select t1.id, t1.event, t1.ts entry, tt.ts exiit ,
                  ((HOUR(tt.ts) - HOUR(t1.ts)) * 60*60)
                  + ((MINUTE(tt.ts) - MINUTE(t1.ts)) * 60)
                  +  (SECOND(tt.ts) - SECOND(t1.ts))
                as diff_seconds,
            t1.rw, tt.rw from t1 join t1 tt on t1.id=tt.id
            and t1.event ='ENTRY' and tt.event='EXIIT'
            and t1.rw+1=tt.rw
            )
            select id, sum(diff_seconds) tot_prod,
            min(entry) mee, max(exiit) mxe,
                     ((HOUR(max(exiit)) - HOUR(min(entry))) * 60*60)
                  + ((MINUTE(max(exiit)) - MINUTE(min(entry))) * 60)
                  +  (SECOND(max(exiit)) - SECOND(min(entry)))
                as diff_seconds_total
            from t2
            group by id
          """).show(100, false)

    println(" Results: using unix_timestamp")
    spark.sql(
      """
            with t1 as ( select id, event, ts, row_number() over(partition by id order by ts) as rw from emp_login),
            t2 as ( select t1.id, t1.event, t1.ts entry, tt.ts exiit ,
                  unix_timestamp(tt.ts)- unix_timestamp(t1.ts)
                as diff_seconds,
            t1.rw, tt.rw from t1 join t1 tt on t1.id=tt.id
            and t1.event ='ENTRY' and tt.event='EXIIT'
            and t1.rw+1=tt.rw
            )
            select id, sum(diff_seconds) tot_prod,
            min(entry) mee, max(exiit) mxe,unix_timestamp(max(exiit)) - unix_timestamp(min(entry)) diff_seconds_total,
            (unix_timestamp(max(exiit)) - unix_timestamp(min(entry)))  - sum(diff_seconds) tot_unprod
            from t2
            group by id
          """).show(100, false)

    println("Results: interval format")

    spark.sql(
      """
            with t1 as ( select id, event, ts, row_number() over(partition by id order by ts) as rw from emp_login),
            t2 as ( select t1.id, t1.event, t1.ts entry, tt.ts exiit ,
                  unix_timestamp(tt.ts)- unix_timestamp(t1.ts)
                as diff_seconds,
            t1.rw, tt.rw from t1 join t1 tt on t1.id=tt.id
            and t1.event ='ENTRY' and tt.event='EXIIT'
            and t1.rw+1=tt.rw
            ),
            t3 (select id, sum(diff_seconds) tot_prod,
            min(entry) mee, max(exiit) mxe,unix_timestamp(max(exiit)) - unix_timestamp(min(entry)) diff_seconds_total,
            (unix_timestamp(max(exiit)) - unix_timestamp(min(entry)))  - sum(diff_seconds) tot_unprod
            from t2
            group by id )
            select id, diff_seconds_total, tot_prod,
            cast(tot_prod/60/60 as int) phr,
            cast( cast(tot_prod/60 as int) - cast(tot_prod/60/60 as int)*60 as int) pmi,
            tot_unprod,
            cast(tot_unprod/60/60 as int) uhr,
            cast( cast(tot_unprod/60 as int) - cast(tot_unprod/60/60 as int)*60 as int) umi
            from t3
          """).show(100, false)

    println("Results: interval format - UTC ")
    val zone = java.time.ZoneId.systemDefault.toString
    spark.sql(
      s"""
            with t1 as ( select id, event, ts, row_number() over(partition by id order by ts) as rw from emp_login),
            t2 as ( select t1.id, t1.event, t1.ts entry, tt.ts exiit ,
                  unix_timestamp(tt.ts)- unix_timestamp(t1.ts)
                as diff_seconds,
            t1.rw, tt.rw from t1 join t1 tt on t1.id=tt.id
            and t1.event ='ENTRY' and tt.event='EXIIT'
            and t1.rw+1=tt.rw
            ),
            t3 (select id, sum(diff_seconds) tot_prod,
            min(entry) mee, max(exiit) mxe,unix_timestamp(max(exiit)) - unix_timestamp(min(entry)) diff_seconds_total,
            (unix_timestamp(max(exiit)) - unix_timestamp(min(entry)))  - sum(diff_seconds) tot_unprod
            from t2
            group by id )
            select id, diff_seconds_total, tot_prod,
            tot_unprod,
            to_timestamp(tot_prod) tot_prod_local_ts,
            to_timestamp(tot_unprod) tot_unprod_local_ts,
            to_utc_timestamp(to_timestamp(tot_prod),'${zone}') tot_prod_utc_ts,
            to_utc_timestamp(to_timestamp(tot_unprod),'${zone}') tot_unprod_utc_ts,
            date_format(to_utc_timestamp(to_timestamp(tot_prod),'${zone}'),'HH:mm:ss') tot_prod_utc_fmt_ts,
            date_format(to_utc_timestamp(to_timestamp(tot_unprod),'${zone}'),'HH:mm:ss') tot_unprod_utc_fmt_ts
            from t3
          """).show(100, false)
  }
}
