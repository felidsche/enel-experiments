/*
  Synthetic analytical query on ECT transaction data from Big data bench
  https://www.benchcouncil.org/BigDataBench/index.html
 */
package de.tu_berlin.dos.arm.spark_utils.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.text.SimpleDateFormat
import java.util.Calendar

object Analytics {
  def main(args: Array[String]): Unit = {
    // constants
    val conf = new AnalyticsArgs(args)
    val appSignature = "Analytics"
    val master = "local" // TODO: change before cluter execution

    val form = new SimpleDateFormat("dd.MM.yyyy_HH:MM:SS")
    val execCal = Calendar.getInstance
    val checkpointTime = form.format(execCal.getTime)

    val sampleSeed = 42

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
      .setMaster(master)

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setCheckpointDir("../checkpoints/" + appSignature + "/" + checkpointTime + "/")

    val spark = SparkSession
      .builder()
      .master(master)
      .appName(appSignature)
      .getOrCreate()

    val orderItemsSchema = StructType(Array(
      StructField("ORDER_ID", IntegerType), // not sure about the which one is first
      StructField("ITEM_ID", IntegerType),
      StructField("GOODS_ID", IntegerType),
      StructField("GOODS_NUMBER", DoubleType),
      StructField("SHOP_PRICE", DoubleType),
      StructField("GOODS_PRICE", DoubleType),
      StructField("GOODS_AMOUNT", DoubleType)
    )
    )

    val orderSchema = StructType(Array(
      StructField("ORDER_ID", IntegerType),
      StructField("ORDER_CODE", LongType),
      StructField("ORDER_DATE", DateType)
    ))

    val orderItems = spark.read
      .options(Map("delimiter" -> "|"))
      .schema(orderItemsSchema)
      .csv(conf.orderItemsInput())
      .sample(fraction = conf.samplingFraction(), seed = sampleSeed)

    val orders = spark.read
      .options(Map("delimiter" -> "|"))
      .schema(orderSchema)
      .csv(conf.ordersInput())
      .sample(fraction = conf.samplingFraction(), seed = sampleSeed)


    println("Starting Analytics...")

    // This import is needed to use the $-notation
    import spark.implicits._

    // random analytics workload
    var df = orders.join(orderItems, usingColumn = "ORDER_ID")

    if (conf.checkpointRdd().equals(1)) {
      println("Checkpointing the DataFrame...")
      df.checkpoint()
    }

    // Get the 5 most recent and expensive orders
    df = df.groupBy($"ORDER_ID", $"ORDER_DATE").agg(
      sum($"GOODS_PRICE").alias("SUM_GOODS_PRICE")
    ).orderBy($"SUM_GOODS_PRICE".desc_nulls_last, $"ORDER_DATE".desc_nulls_last)

    df.show(5)

    spark.stop()

    println("Finished Analytics.")
  }

}

class AnalyticsArgs(a: Seq[String]) extends ScallopConf(a) {

  val orderItemsInput: ScallopOption[String] = trailArg[String](required = true, name = "<orderItemsInput>",
    descr = "Order Items input file").map(_.toLowerCase)

  val ordersInput: ScallopOption[String] = trailArg[String](required = true, name = "<ordersInput>",
    descr = "Order input file").map(_.toLowerCase)

  val samplingFraction: ScallopOption[Double] = opt[Double](noshort = true, default = Option(0.001),
    descr = "Whether to checkpoint the RDD before aggregation or not")

  // interpreted as boolean
  val checkpointRdd: ScallopOption[Int] = opt[Int](noshort = true, default = Option(0),
    descr = "Whether to checkpoint the RDD before aggregation or not")

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
