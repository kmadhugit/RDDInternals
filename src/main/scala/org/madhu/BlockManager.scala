package org.madhu


import org.apache.spark.storage.RDDBlockId
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

object BlockManager {


  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val baseRDD = sc.parallelize("00 11 22 33 44 55 66 77 88 99".split(" ").toSeq).repartition(4).cache()
    baseRDD.setName("baseRDD")

    //check whether its in cache
    val blockManager = SparkEnv.get.blockManager
    val key = RDDBlockId(baseRDD.id, 0)

    println("before evaluation " +blockManager.get(key))

    baseRDD.count()

    val x = blockManager.get(key)
    x.get.data.foreach(println(_))

    println("MADHU BLOCK TYPE" + x.getClass.getSimpleName)

  }

}

