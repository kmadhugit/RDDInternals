package org.madhu

import java.util.Random
import scala.math.exp
import org.apache.spark._

/**
 * Created by kmadhu on 5/11/15.
 */
object LinearRegressionArray {

  def muls(x: Array[Double], c: Double): Array[Double] = {
    Array.tabulate(x.length)(i => x(i) * c)
  }

  def add(x: Array[Double], y: Array[Double]): Array[Double] = {
    Array.tabulate(x.length)(i => x(i) + y(i))
  }

  def subself(x: Array[Double], y: Array[Double]) = {
    var i = 0
    while (i < x.length) {
      x(i) -= y(i)
      i += 1
    }
  }

  def dot(x: Array[Double], y: Array[Double]): Double = {
    var ans = 0.0
    var i = 0
    while (i < x.length) {
      ans += x(i) * y(i)
      i += 1
    }
    ans
  }

  def sum(x: Array[Double], y: Array[Double]): Double = {
    var ans = 0.0
    var i = 0
    while (i < x.length) {
      ans += x(i) + y(i)
      i += 1
    }
    ans
  }

  def printlnArray(s: String, x: Array[Double]) = {
    print(s)
    x.foreach(e => print(e + " "))
    println()
  }

 case class DataPoint(x: Array[Double], y: Double)

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("SparkLR")
    val sc = new SparkContext(sparkConf)

    val N = 2
    val R = 0.7
    val D = 5
    val ITERATIONS = 2

    val dataset = Array(DataPoint(Array(1.0, 2.0, 3.0, 4.0, 5.0), 1.0),
      DataPoint(Array(1.0, 2.0, 3.0, 4.0, 5.0), 1.0) )

    val points = sc.parallelize(dataset, 2).cache()

/*    var w = Array.fill(D) {
      math.random
    }*/

    var w = List[Double](0.9706523792091876, 0.9689950601959192, 0.8871562128145087, 0.64360369303987, 0.905075008043427 ).toArray

    println("inital w: { " + w.toList)

    //points.map { p => muls(p.x, (1 / (1 + exp(-p.y * (dot(w, p.x)))) - 1) * p.y) }.foreach(x => println(x.toList))


    val now = System.nanoTime
    for (i <- 1 to 10) {
      val gradient = points.map { p => muls(p.x, (1 / (1 + exp(-p.y * (dot(w, p.x)))) - 1) * p.y) }.reduce(add(_, _))
      println("HERE-->" + gradient.toList)
      subself(w, gradient)
    }
    val elapsed = (System.nanoTime - now) / 1000
    printlnArray("final  w: ", w)
    //println("%d microseconds".format(micros))
  }


}


