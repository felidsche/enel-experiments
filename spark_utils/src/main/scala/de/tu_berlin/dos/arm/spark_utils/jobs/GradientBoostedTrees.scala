package de.tu_berlin.dos.arm.spark_utils.jobs

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.text.SimpleDateFormat
import java.util.Calendar

object GradientBoostedTrees {

  def main(args: Array[String]): Unit = {

    val conf = new GradientBoostedTreesArgs(args)
    val appSignature = "GradientBoostedTrees"

    val form = new SimpleDateFormat("dd.MM.yyyy_HH:MM:SS")
    val execCal = Calendar.getInstance
    val checkpointTime = form.format(execCal.getTime)

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
      .setMaster("local") // TODO: remove before cluter execution

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setCheckpointDir("checkpoints/" + appSignature + "/" + checkpointTime + "/")

    val spark = SparkSession
      .builder
      .appName(appSignature)
      .getOrCreate()


    println("Start GBT Workload...")

    // we need DataFrames since Checkpoints are not available with RDD based MLLlib
    var data = spark.read.format("csv")
      .option("delimiter", ",")
      .load(conf.input())
      .toDF()

    // cast all columns to Double
    data = data.select(data.columns.map(c => col(c).cast(DoubleType)): _*)

    // rename the first column to be the "label" column
    data = data.withColumnRenamed("_c0", "label")
    // compose a vector from all columns except the label (first one)
    val featureColumns = data.columns.drop(1)

    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features")

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a GradientBoostedTrees model.
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(conf.iterations())

    if (conf.checkpoint().equals(1)) {
      println("Checkpointing GBT every" + conf.checkpointInterval() + " iterations...")
      gbt.setCheckpointInterval(conf.checkpointInterval()) // defines after how many iterations to checkpoint
    }


    val pipeline = new Pipeline()
      .setStages(Array(assembler, gbt))

    val model = pipeline.fit(trainingData)

    println("Start GBT predictions...")
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)

    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
    println(s"Learned regression GBT model:\n ${model.stages(1).asInstanceOf[GBTRegressionModel].toDebugString}")

    sparkContext.stop()
  }
}

class GradientBoostedTreesArgs(a: Seq[String]) extends ScallopConf(a) {
  val input: ScallopOption[String] = trailArg[String](required = true, name = "<input>",
    descr = "Input file").map(_.toLowerCase)

  val iterations: ScallopOption[Int] = opt[Int](noshort = true, default = Option(100),
    descr = "Amount of iterations")

  // interpreted as boolean
  val checkpoint: ScallopOption[Int] = opt[Int](noshort = true, default = Option(0),
    descr = "Whether to checkpoint GBT every `checkpointInterval` iterations or not")

  val checkpointInterval: ScallopOption[Int] = opt[Int](noshort = true, default = Option(-1),
    descr = "Interval of checkpoints")


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
