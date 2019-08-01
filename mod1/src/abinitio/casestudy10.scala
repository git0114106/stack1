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

object casestudy10 {

  def main(argsi: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Spark_Abinitio_case_study10").master("local[*]").getOrCreate()
    import spark.implicits._

    //Read files
    val drop_cols = List("dummy1","dummy2","dummy3")
    val acct = spark.read.format("csv").option("delimiter", "|").load("mod1/abinitio/in/case10_acct.txt")
                      .toDF("cust_no", "cust_name", "join_date", "credit_limit").withColumn("credit_limit",$"credit_limit".cast("double"))
    val prior = spark.read.format("csv").option("delimiter", "|").load("mod1/abinitio/in/case10_prior.txt")
      .toDF("cust_no", "bill_amt", "payment", "due_amt")
    val tran = spark.read.format("csv").option("delimiter", "~").load("mod1/abinitio/in/case10_tran.txt")
      .toDF("cust_no","dummy1", "tran_id", "dummy2","tran_date", "dummy3","tran_amt")
        .drop(drop_cols:_*) // .drop($"dummy1").drop($"dummy2").drop($"dummy3")
      .withColumn("tran_amt",$"tran_amt".cast("double"))
    acct.show(false)
    prior.show(false)
    tran.show(false)

    val bill1 = acct.alias("t1").join(tran.alias("t2"),$"t1.cust_no" === $"t2.cust_no", "leftouter")
          .select($"t1.cust_no",$"t1.cust_name",$"t1.credit_limit",$"t2.tran_amt")
          .groupBy($"t1.cust_no").agg( sum($"tran_amt").as("tran_amt"), max($"credit_limit").as("credit_limit") )
          .withColumn("over_credit_limit_charge", when($"tran_amt" >= $"credit_limit", $"credit_limit" * lit(0.1)).otherwise(lit(0)))

    bill1.show(false)

    val bill2 = bill1.alias("t1").join(prior.alias("t2"), $"t1.cust_no" === $"t2.cust_no", "leftouter")
                 .select($"t1.cust_no",$"t1.tran_amt",$"t1.credit_limit", $"t1.over_credit_limit_charge",$"t2.due_amt")
    bill2.show(false)
    val bill_final = bill2.withColumn("finance_charges",when($"due_amt" > 0, $"due_amt" * lit(0.2)).otherwise(lit(0)) )
                          .withColumn("bill_amt", coalesce($"tran_amt",lit(0)) + $"finance_charges" + $"over_credit_limit_charge" + coalesce($"due_amt",lit(0)))

    bill_final.show(false)

  }

}
