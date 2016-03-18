package org.madhu

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kmadhu on 19/1/16.
 */
object junk2 {

  def main(args: Array[String]) {

    def g() = {
      println("G");
      1 to 10;
    }

    val conf = new SparkConf().setMaster("local").setAppName("runJob")
    val sc = new SparkContext(conf)
    val p = sc.parallelize(1 to 2).map(x => { Thread.dumpStack(); x*2}).cache()


    println(p.collect.toList)

  }
}


