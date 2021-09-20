package de.tu_berlin.dos.arm.spark_utils.datagens

import java.io.{BufferedWriter, File, FileWriter}
import java.util.concurrent.ThreadLocalRandom
import scala.math.pow


object SGDDataGeneratorLocal {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      Console.err.println("Usage: SGDDataGeneratorLocal <samples> <dimension> <output>")
      System.exit(-1)
    }

    println("Start generating SGD data (for GBT) locally...")

    val m = args(0).toInt
    val n = args(1).toInt
    val outputPath = args(2)


    val file = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(file))

    for (_ <- 0 until m) {
      val x = ThreadLocalRandom.current().nextDouble() * 10
      val noise = ThreadLocalRandom.current().nextGaussian() * 3

      // generate the function value with added gaussian noise
      val label = function(x) + noise

      // generate a vandermatrix from x
      val vector = polyvander(x, n - 1)

      // write to file in csv format (label, feature1, feature2, featureN)
      bw.write(label + "," + vector.mkString(",") + "\n")
    }

    bw.close()

    println("Finish generating SGD data (for GBT) locally...")

  }

  def polyvander(x: Double, order: Int): Array[Double] = {
    (0 to order).map(pow(x, _)).toArray
  }

  def function(x: Double): Double = {
    2 * x + 10
  }
}
