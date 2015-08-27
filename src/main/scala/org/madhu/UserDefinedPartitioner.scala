package org.madhu

/**
 * Created by kmadhu on 27/8/15.
 */

import org.apache.spark.{SparkConf, HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.SparkContext._


class myPartitioner extends Partitioner {
  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int =
    key match {
      case null => 0
      case _ => {
        val keyValue = key.toString.toInt
        if(keyValue>=2) 1 else 0
      }
    }
}


object UserDefinedPartitioner {

  def printPartitions[T](P : Int, it : Iterator[T])  =
  {
    it.foreach({case (x,y) => println("Partition No = " + P + " Key = " + x + " Value = " + y) })
    it
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1,"A"),(2,"B"),(3,"C"),(4,"D")))

    println("Default Hash Partitioner")
    val x = rdd.repartition(2).mapPartitionsWithIndex(printPartitions[Tuple2[Int,String]])
    x.count()
    println("myPartitioner")
    val y = rdd.partitionBy(new myPartitioner).mapPartitionsWithIndex(printPartitions[Tuple2[Int,String]])
    y.count()
  }
}

