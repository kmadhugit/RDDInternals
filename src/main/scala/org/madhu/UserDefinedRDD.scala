package org.madhu

/**
 * Created by madhu on 27/2/15.
 */

import org.apache.spark.rdd.RDD
import org.apache.spark._


class myPartition(val index: Int, val parent : Partition) extends Partition with Serializable {
  // Put all the additional data/methods you need to pass to compute function, to compute the given partition. Also store
  // parent partition data, because you need to get parent's partition data to manipulate.
  def extraData() = {
    println("NO EXTRA DATA :-) FOR PARTITION NO " + index)
  }

  def preferredLocations : Seq[String] = {
    println("Preferred Location called for Partition no " + index)
    Nil
  }
}

class DecorateRDD(parentRDD:RDD[String], a:String, b:String) extends RDD[String](parentRDD){

  /*
    This would be called during runtask. This is the actual function, it should return the data for the
    given partition after manipulation. Get the parent's data using
      firstParent[T].iterator(split, context) == this.dependencies.head.rdd == prev
   */
  override def compute(split: Partition, context: TaskContext): Iterator[String] =  {
    val myP = split.asInstanceOf[myPartition]
    myP.extraData()
    val parentPartitionData =  firstParent[String].iterator(myP.parent, context)
    val myPartitionData = parentPartitionData.map(data => { a + data + b })
    myPartitionData
  }

  /*
   This is just an array of partitions.This will be called on driver context only once and then would be passed as
   argument to compute function.
  */
  override protected def getPartitions: Array[Partition] = {
    val mypartitions = for(x <- firstParent[String].partitions) yield new myPartition(x.index,x)
    mypartitions.map(_.asInstanceOf[Partition])
  }

  // This would be called by Task Scheduler.
  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[myPartition].preferredLocations
  }

  override def getDependencies: Seq[Dependency[_]] = {
    //We just have one to one dependency from a single parent for each partition.
    println("Get dependencies is called")
    List(new OneToOneDependency(parentRDD))
  }

}

class RDDConverterClass(rdd:RDD[String]) {
  def decorate(a:String, b:String) = new DecorateRDD(rdd,a,b)
}

object UserDefinedRDD {


  implicit def RDDConverFunc(rdd: RDD[String]) = new RDDConverterClass(rdd)

  def main(args : Array[String]) =
  {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val baseRDD = sc.parallelize("00 11 22 33 44 55 66 77 88 99".split(" ").toSeq).repartition(4)
    val newRDD  = new DecorateRDD(baseRDD,"{","}")
    println(baseRDD.collect().toList)
    println(newRDD.collect().toList)

    val y = baseRDD.decorate("<",">")
    println(y.collect().toList)
  }
}
