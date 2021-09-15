/*
  Synthetic analytical query on ECT transaction data from Big data bench
  https://www.benchcouncil.org/BigDataBench/index.html
 */
package de.tu_berlin.dos.arm.spark_utils.jobs
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.text.SimpleDateFormat
import java.util.Calendar

object Analytics {
  def main(args: Array[String]): Unit = {
    // constants
    val conf = new AnalyticsArgs(args)
    val appSignature = "Analytics"

    val form = new SimpleDateFormat("dd.MM.yyyy_HH:MM:SS")
    val execCal = Calendar.getInstance
    val checkpointTime = form.format(execCal.getTime)

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(appSignature)
      .getOrCreate()

    val orderItems = spark.read.options(Map("header" -> "true", "delimiter" -> "\t", "inferSchema" -> "true")).csv(conf.orderItemsInput())
    val orders = spark.read.options(Map("header" -> "true", "delimiter" -> "\t", "inferSchema" -> "true")).csv(conf.ordersInput())
    println("Starting Analytics...")
    println("Orders schema: "+orders.schema)
    orders.summary().show()
    println("orderItems schema: "+orderItems.schema)
    orderItems.summary().show()
    // This import is needed to use the $-notation
    import spark.implicits._

    // random analytics workload
    var df = orders.join(orderItems, usingColumn = "ORDER_ID")
    var df1 = df.filter($"GOODS_ID".like("1018544"))
    var df2 = df.filter($"GOODS_ID".like("1016104"))

    df1 = df1.groupBy($"BUYER_ID", $"GOODS_ID", $"SHOP_PRICE").agg(
      min($"GOODS_PRICE").alias("MIN_GOODS_PRICE"),
      mean($"GOODS_PRICE").alias("MEAN_GOODS_PRICE"),
      max($"GOODS_PRICE").alias("MAX_GOODS_PRICE"),
      min($"GOODS_AMOUNT").alias("MIN_GOODS_AMOUNT"),
      mean($"GOODS_AMOUNT").alias("MEAN_GOODS_AMOUNT"),
      max($"GOODS_AMOUNT").alias("MAX_GOODS_AMOUNT")
    ).orderBy($"SHOP_PRICE".desc_nulls_last, $"BUYER_ID".desc_nulls_last)


    df2 = df2.groupBy($"CREATE_DT", $"CREATE_IP", $"PAY_DT").agg(
      sum($"GOODS_PRICE").alias("SUM_GOODS_PRICE"),
      count($"GOODS_AMOUNT").alias("COUNT_GOODS_AMOUNT"),
      avg($"GOODS_PRICE").alias("AVG_GOODS_PRICE")
    ).orderBy($"CREATE_DT".asc_nulls_last, $"PAY_DT".asc_nulls_last)

    val unionDf = df1.unionByName(df2, allowMissingColumns = true)

    unionDf.show()

    spark.stop()
    println("Finished Analytics.")
  }

}

class AnalyticsArgs(a: Seq[String]) extends ScallopConf(a) {

  val orderItemsInput: ScallopOption[String] = trailArg[String](required = true, name = "<orderItemsInput>",
    descr = "Order Items input file").map(_.toLowerCase)

  val ordersInput: ScallopOption[String] = trailArg[String](required = true, name = "<ordersInput>",
    descr = "Order input file").map(_.toLowerCase)

  val checkpointRDD: ScallopOption[Int] = opt[Int](noshort = true, default = Option(10),
    descr = "The ID of the RDD to checkpoint")
  override def onError(e: Throwable): Unit = e match {
    case ScallopException(message) =>
      println(message)
      println()
      printHelp()
      System.exit(1)
    case other => super.onError(e)
  }

  verify()
}
