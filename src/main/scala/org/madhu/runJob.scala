package org.madhu
import org.apache.spark.{SparkConf, TaskContext, Logging, SparkContext}

/**
 * Created by madhu on 24/3/15.
 */

object runJob {

  var jobResult: Int = 0

  val processPartition_v1 = (it : Iterator[String]) => {
    print("processPartition_v1 input = " );
    val x = it.map((x)=>{print(x + " ");x.length}).sum
    println(" output = " + x)
    x
  }


  val processPartition_v2 = (context: TaskContext,it : Iterator[String]) => {
    print("processPartition_v2 input = " );
    val x = it.map((x)=>{print(x + " ");x.length}).sum
    println(" output = " + x)
    x
  }

  val resultFunc = (index: Int, taskResult: Int) => {
    jobResult = jobResult + taskResult
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("runJob")
    val sc = new SparkContext(conf)
    sc.setCallSite("SANJAY CALLSITE")

    val baseRDD = sc.parallelize("00 11 22 33 44 55 66 77 88 99".split(" ").toSeq).repartition(4)

    //println("MADHU BASE RDD REDUCE => " + baseRDD.map(_.length).reduce(_ + _))

    //val results = sc.runJob(baseRDD, processPartition_v1)
    //println("MADHU RUNJOB V1 RESULTS ==> " + results.toList)

    sc.runJob(baseRDD,processPartition_v2,resultFunc)
    println("MADHU RUNJOB V2 RESULTS ==> " + jobResult)
  }

}

