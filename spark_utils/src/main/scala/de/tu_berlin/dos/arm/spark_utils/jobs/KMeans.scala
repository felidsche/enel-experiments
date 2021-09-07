package de.tu_berlin.dos.arm.spark_utils.jobs

/*
 * KMeans workload for experiments
 */

import org.apache.spark.mllib.clustering
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.concurrent.duration._

object KMeans {
  val MLLibKMeans: clustering.KMeans.type = org.apache.spark.mllib.clustering.KMeans

  def main(args: Array[String]): Unit = {

    val conf = new KMeansArgs(args)
    val appSignature = "KMeans"

    val splits = 2
    val sparkConf = new SparkConf()
      .setAppName(appSignature)
      .setMaster("local") // TODO: remove before cluter execution

    val form = new SimpleDateFormat("dd.MM.yyyy_HH:MM:SS");
    val execCal = Calendar.getInstance

    val executionDate = form.format(execCal.getTime);

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setCheckpointDir("../../checkpoints/KMeans/" + executionDate + "/")

    println("Start KMeans training...")
    // Load and parse the data
    val data = sparkContext.textFile(conf.input(), splits)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))

    val checkpointStartCal =  Calendar.getInstance
    val checkpointStartMillis = checkpointStartCal.getTimeInMillis
    println("Start checkpoint at " + checkpointStartMillis.toString)

    // add a checkpoint on the map task to resume here if clustering fails
    parsedData.checkpoint()

    val checkpointEndCal =  Calendar.getInstance
    val checkpointEndMillis = checkpointEndCal.getTimeInMillis
    println("End checkpoint at " + checkpointEndMillis.toString)

    val checkpointDuration = Duration(checkpointEndMillis - checkpointStartMillis, MILLISECONDS)
    println("Checkpoint duration " + checkpointDuration.toString)

    //TODO: add a failure here
    val clusters = new org.apache.spark.mllib.clustering.KMeans()
      .setEpsilon(0)
      .setK(conf.k())
      .setMaxIterations(conf.iterations())
      .run(parsedData)


    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    clusters.clusterCenters.foreach(v => {
      println(v)
    })

    sparkContext.stop()
  }
}

class KMeansArgs(a: Seq[String]) extends ScallopConf(a) {
  val input: ScallopOption[String] = trailArg[String](required = true, name = "<input>",
    descr = "Input file").map(_.toLowerCase)

  val k: ScallopOption[Int] = opt[Int](required = true, descr = "Amount of clusters")
  val iterations: ScallopOption[Int] = opt[Int](noshort = true, default = Option(100),
    descr = "Amount of KMeans iterations")

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