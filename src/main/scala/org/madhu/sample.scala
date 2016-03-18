package org.madhu

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kmadhu on 15/12/15.
 */
object sample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("runJob")
    val sc = new SparkContext(conf)

    val r1 = sc.parallelize(1 to 10, 2)

    val r2 = r1.map( x => ( x/2, x))

    val r3 = r2.reduceByKey(_ + _).repartition(2)

    println(r3.collect.toList)


    val x= readLine()


  }
}
