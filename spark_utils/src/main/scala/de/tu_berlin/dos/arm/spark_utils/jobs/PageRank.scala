/**
 * This program is based on examples of spark-0.8.0-incubating
 * The original source file is: org.apache.spark.examples.SparkPageRank
 */

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
package de.tu_berlin.dos.arm.spark_utils.jobs

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.text.SimpleDateFormat
import java.util.Calendar

object PageRank {
  def main(args: Array[String]): Unit = {

    val conf = new PageRankArgs(args)
    val input = conf.input()
    val iters = conf.iterations()
    val slices = conf.slices()
    val save_path = conf.savePath()

    val appSignature = "PageRank"
    val form = new SimpleDateFormat("dd.MM.yyyy_HH:MM:SS")
    val execCal = Calendar.getInstance
    val checkpointTime = form.format(execCal.getTime)

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
      .setMaster("local") // TODO: remove before cluter execution

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setCheckpointDir("checkpoints/" + appSignature + "/" + checkpointTime + "/")

    // load data
    val lines = sparkContext.textFile(input, slices)

    println("Start PageRank Workload...")

    // directed edges: (from, (to1, to2, to3))
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    println(links.count.toString + " links loaded.")
    // rank values are initialised with 1.0
    var ranks = links.mapValues(v => 1.0).persist(StorageLevel.MEMORY_AND_DISK)
    // possible checkpoint also for "ranks"

    for (i <- 1 to iters) {
      // calculate contribution to desti-urls
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }.persist(StorageLevel.MEMORY_AND_DISK)

      if (conf.checkpoint().equals(1)) {
        contribs.checkpoint()
      }

      // This may lead to points' miss if a page have no link-in
      // add all contribs together, then calculate new ranks
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

    }

    println("Finish PageRank Workload...")

    //TODO: make this work
    /* show results
    println("Result saved to: " + save_path)
    ranks.saveAsTextFile(path = "file:///" + save_path)
    */
  }
}

class PageRankArgs(a: Seq[String]) extends ScallopConf(a) {
  val input: ScallopOption[String] = trailArg[String](required = true, name = "<input>",
    descr = "Input file").map(_.toLowerCase)

  val slices: ScallopOption[Int] = opt[Int](noshort = true, default = Option(1),
    descr = "Amount of slices")

  val savePath: ScallopOption[String] = opt[String](noshort = true,
    descr = "Where to save the contributions")

  val iterations: ScallopOption[Int] = opt[Int](noshort = true, default = Option(100),
    descr = "Amount of iterations")

  // interpreted as boolean
  val checkpoint: ScallopOption[Int] = opt[Int](noshort = true, default = Option(0),
    descr = "Whether to checkpoint PageRank contributions or not")


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

