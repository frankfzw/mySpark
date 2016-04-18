package org.apache.spark.examples

import java.util.Random

import breeze.linalg.{DenseVector, Vector}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.examples.SparkLR.DataPoint

import scala.math._

/**
 * Created by frankfzw on 16-1-12.
 */
object MyLR {
  var N = 100000  // Number of data points
  val D = 10   // Numer of dimensions
  val R = 0.7  // Scaling factor
  var ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def generateData: Array[DataPoint] = {
    def generatePoint(i: Int): DataPoint = {
      val y = if (i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D){rand.nextGaussian + y * R}
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
        |org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
        |for more conventional use.
      """.stripMargin)
  }

  def showUsage(): Unit = {
    System.err.println(
      """MyLR number of pointers number of interations
      """.stripMargin)
  }

  def main(args: Array[String]) {

    showWarning()
    showUsage()

    val sparkConf = new SparkConf().setAppName("MyLR")
    val sc = new SparkContext(sparkConf)
    val numSlices = if (args.length > 0) args(0).toInt else 2
    N = if (args.length > 1) args(1).toInt else 100000
    ITERATIONS = if (args.length > 2) args(2).toInt else 5
    val points = sc.parallelize(generateData, numSlices).cache()

    // Initialize w to a random value
    var w = DenseVector.fill(D){2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)

    sc.stop()
  }

}
