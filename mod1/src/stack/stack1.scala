package stack

import scala.math.BigInt
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql._
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object stack1 {
  def main(argsi: Array[String]) {
    var args = argsi
    args = Array("C:\\Users\\an06599\\spark_training2\\stack1_scala\\in\\npd.csv", "C:\\Users\\an06599\\Desktop\\mesears41_utf8.dat") // comment for packaging
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Spark_processing for Hbase").master("local[*]").getOrCreate()
    //val conf = new SparkConf().setAppName("CBNA_MSTR").setMaster("local[*]") // Local
    val conf = new SparkConf().setAppName("CBNA_MSTR") //Server
    // val sc = new SparkContext(conf)
    import spark.implicits._


    val df = Seq(
      ("161","xyz Limited","U.K."),
      ("262","ABC  Limited","U.K."),
      ("165","Sons & Sons","U.K."),
      ("361","TÜV GmbH","Germany"),
      ("462","Mueller GmbH","Germany"),
      ("369","Schneider AG","Germany"),
      ("467","Sahm UG","Germany")
    ).toDF("ID","customers","country")

    val list1 =Array("16", "26")
    val list2 =Array("36", "46")

    df.show(false)
    df.createOrReplaceTempView("secil")
    spark.sql(
      """ with t1 ( select id, customers, country, array('16','26') as a1, array('36','46') as a2 from secil),
         t2 (select id, customers, country,  filter(a1, x -> id like x||'%') a1f,  filter(a2, x -> id like x||'%') a2f from t1),
         t3 (select id, customers, country, a1f, a2f,
                   case when size(a1f) > 0 then 1 else 0 end a1r,
                   case when size(a2f) > 0 then 2 else 0 end a2r
                   from t2)
         select id, customers, country, a1f, a2f, a1r, a2r, a1r+a2r as Cat_ID from t3
      """.stripMargin).show(false)

    /*
    val df = Seq((1, 5, 2019,1,20),(2, 4, 2019,2,18),
    (3, 3, 2019,3,21),(4, 2, 2019,4,20),
    (5, 1, 2019,5,1),(6, 52, 2018,6,2),
    (7, 51, 2018,7,3)).toDF("product_id", "week", "year","rank","price")
    //df.show(false)
    df.createOrReplaceTempView("sales")
    //spark.sql(""" select * from sales """).show(false)

    val df2 = spark.sql("""
              select product_id, week, year, price,
              count(*) over(order by year desc, week desc rows between 1 following and 3 following  ) as count_row,
              lag(price) over(order by year desc, week desc ) as lag1_price,
              sum(price) over(order by year desc, week desc rows between 2 preceding and 2 preceding ) as lag2_price,
              max(price) over(order by year desc, week desc rows between 1 following and 3 following  ) as max_price1 from sales
      """)
    df2.show(false)
    df2.createOrReplaceTempView("sales_inner")
    spark.sql("""
              select product_id, week, year, price,
              case
                 when count_row=2 then greatest(price,max_price1)
                 when count_row=1 then greatest(price,lag1_price,max_price1)
                 when count_row=0 then greatest(price,lag1_price,lag2_price)
                 else  max_price1
              end as max_price
             from sales_inner
      """).show(false)
*/
    /*
    val df = Seq((101,Array("a","b","c","d","e")),(102,Array("q","w","e")),(103,Array("z","x","w","t","e","q","s"))).toDF("emp","list")
    df.show(false)
    val df2 = df.withColumn("list_size_arr",  array_repeat(lit(1), ceil(size('list)/3).cast("int")) )
    val df3 = df2.select(col("*"),posexplode('list_size_arr))
    val udf_slice = udf( (x:Seq[String],start:Int, end:Int )  => x.slice(start,end) )
    df3.withColumn("newlist",udf_slice('list,'pos*3, ('pos+1)*3  )).select($"emp", $"newlist").show(false)
    val df4 = df3.withColumn("newlist",udf_slice('list,'pos*3, ('pos+1)*3  )).select($"emp", $"newlist")
    df4.select($"emp", $"newlist"(0).as("col1"), $"newlist"(1).as("col2"), $"newlist"(2).as("col3") ).show(false)
*/

  }
}
