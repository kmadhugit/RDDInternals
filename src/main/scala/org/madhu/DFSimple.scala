package org.madhu

import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by kmadhu on 15/3/16.
 */


object DFSimple {

  case class data(i : Int, j: Int, k: Int)

  def main(args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sc.parallelize(1 to 10, 4).map(i => data(i,i*10,i*100)).toDF

    import org.apache.spark.sql.functions.udf


    val coli = df("i")
    val colj = df("j")
    val colk = df("k")



    df.select(coli,colj).show();



    val rdd = df.toJavaRDD

    println(rdd.collect());

  }

}
