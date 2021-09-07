package de.tu_berlin.dos.arm.spark_utils.datagens

/*
 * Data generation for KMeans clustering
 * writes to local filesystem
 */

import breeze.linalg.DenseVector
import breeze.stats.distributions.Rand
case class MeanConf(mean: DenseVector[Double], stdDev: Double)
import java.io.{File, PrintWriter}

object KMeansDataGenerator {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("KMeansDataGenerator <samples> <cluster> <output>")
      System.exit(1)
    }
    /*
    default conf: 55555555 55 ../../../../samples/Kmeans.txt
     */
    val n = args(0).toInt
    val k = args(1).toInt
    val outputPath = args(2)

    val dim = 2
    val stdDev = .012

    Rand.generator.setSeed(0)

    val centers = uniformRandomCenters(dim, k, stdDev)
    val centerDistribution = Rand.choose(centers)

    val file = new File(outputPath)
    val pw = new PrintWriter(file)

    (1 to n).foreach(_ => {
      val MeanConf(mean, stdDev) = centerDistribution.draw()
      val p = mean + DenseVector.rand[Double](mean.length, Rand.gaussian(0, stdDev))
      pw.write(p.toArray.mkString(" ") + "\n")
    })
    pw.close()

  }

  def uniformRandomCenters(dim: Int, k: Int, stdDev: Double): Seq[MeanConf] = {
    (1 to k).map(_ => {
      val mean = DenseVector.rand[Double](dim)
      MeanConf(mean, stdDev)
    })
  }
}