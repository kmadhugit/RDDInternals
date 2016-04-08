package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by madhusudanan on 31/03/16.
  */
object DFInternal {

case class data(i : Int, j: Int, k: Int)

def main(args : Array[String]): Unit = {

  val conf = new SparkConf ().setMaster ("local").setAppName ("test")
  val sc = new SparkContext (conf)
  val sqlContext = new org.apache.spark.sql.SQLContext (sc)

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val df = sc.parallelize (1 to 10, 4).map (i => data ((i)/2, i * 10, i * 10) ).toDF("a","b","c")

  df.groupBy("a")

  import org.apache.spark.sql.functions.udf

  df.groupBy("a").agg(col("a"),col("b"),max("b")).show()

//  val rdd = df.queryExecution.toRdd.map(x => col.eval(x)).foreach(println(_))


}


}