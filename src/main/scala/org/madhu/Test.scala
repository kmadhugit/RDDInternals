package org.madhu

import org.apache.spark.{SparkEnv, HashPartitioner, SparkContext, SparkConf}

/**
 * Created by kmadhu on 8/9/15.
 */
object Test {
  def foo() =
  {
    val x = java.lang.Thread.currentThread.getStackTrace().filter(!_.getMethodName.equals("getStackTrace")).
      filter( !_.getMethodName.startsWith("log"))
    val methodNames = "\n" + x.map(_.getMethodName.split('$').last).foldLeft(" ")((x,y)=>x.toString + "<-"+ y)
    println(x.toList)
    println(methodNames)
  }

/*  def main(args : Array[String]) =
  {
    foo();
    println("as$ecd$asdfs$$method".split('$').last)
    val conf = new SparkConf().setMaster("spark://kmadhu-ThinkPad-T420:7077").setAppName("runJob")
    val sc = new SparkContext(conf)


    val rdd1 = sc.parallelize("00 11 22 33 44 55 66 77 88 99".split(" ").toSeq).setName("A1")
    val rdd2 = rdd1.repartition(4).setName("A2")
    val rdd3 = rdd2.repartition(3).setName("A3")
    val rdd4 = rdd3.repartition(2).setName("A4")
    val rdd5 = rdd4.repartition(1).setName("A5")
    val results = rdd5.collect()
    println("results = " + rdd1.collect.toList)

  }*/

  def main(args : Array[String]): Unit = {

    val conf = new SparkConf().setMaster("spark://kmadhu-ThinkPad-T420:7077").setAppName("test").
    set("spark.executor.memory", "1024MB").
    set("spark.cores.max", "2")
    val sc = new SparkContext(conf)

    println("STOP####################")

    val partitionedRDD = sc.parallelize(1 to 6).map(x=>(x,x)).partitionBy(new HashPartitioner(6)).values
    partitionedRDD.setName("BASE RDD")

    println("Number of StorageStatus are " + sc.getExecutorStorageStatus.length);
    sc.getExecutorStorageStatus.foreach(x=>println("Block mgr id = " + x.blockManagerId))

    println("Number of MemoryStatus are " + sc.getExecutorMemoryStatus.keys.size);
    sc.getExecutorMemoryStatus.foreach(x => println(x._1 +   " ( " +  x._2._1 + ", " + x._2._2 + ")"))

    partitionedRDD.count()

    println("Number of StorageStatus are " + sc.getExecutorStorageStatus.length);
    sc.getExecutorStorageStatus.foreach(x=>println("Block mgr id = " + x.blockManagerId))

    println("Number of MemoryStatus are " + sc.getExecutorMemoryStatus.keys.size);
    sc.getExecutorMemoryStatus.foreach(x => println(x._1 +   " ( " +  x._2._1 + ", " + x._2._2 + ")"))

    println("Driver Block MGR ID " + SparkEnv.get.blockManager.blockManagerId)

    println("COMPLETED$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    val x = readLine()
  }


}
