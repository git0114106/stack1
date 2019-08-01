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

object casestudy3 {

  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Spark_Abinitio_case_study3").master("local[*]").getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").load("mod1/abinitio/in/case3.txt").toDF("acct_id", "trans_dt", "trans_amt")
    df.show(false)
    val df2 = df.groupBy("acct_id").agg(collect_list(struct('trans_dt,'trans_amt)).as("trans_vec"))
    df2.show(false)
    df2.printSchema()

    def gen_rows(x:Seq[Row])={
      val dt = x.map ( a => a.getAs[String]("trans_dt"))
      val amt = x.map( p => p.getAs[String]("trans_amt"))
      for( i <- (0 until x.length))
        yield (dt(i),amt(i),i+1)
    }
    val udf_gen_rows = udf( gen_rows(_:Seq[Row]) )

    val df3 = df2.withColumn("row1",udf_gen_rows('trans_vec)).withColumn("row2",explode('row1))
    df3.printSchema()
    df3.show(false)

    val df4 = df3.select($"acct_id",$"row2._1".as("tran_dt"), $"row2._2".as("tran_amt"), $"row2._3".as("seq_num"))



    df4.show(false)

    val df5 = df2.select(col("*"),posexplode($"trans_vec").as(Seq("pos","row2")))
    val df6 = df5.select($"acct_id",$"row2.trans_dt".as("tran_dt"), $"row2.trans_amt".as("trans_amt"), $"pos").withColumn("seq_num", lit(1) + $"pos")

    df6.show(false)

    println("Using SQL")
    df.createOrReplaceTempView("case3")
    spark.sql("""
                 select *, row_number() over(partition by acct_id order by trans_dt) seq_no from case3
        """).show(100,false)
    println("using df Window functions")
    df.withColumn("seq_num", row_number() over(Window.partitionBy('acct_id).orderBy('trans_dt))).show(100,false)
  }
}
