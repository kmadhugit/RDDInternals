package org.madhu


import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{StorageLevel, RDDBlockId}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

object RDDStore {
  var i = 10;
}

object BlockManager {

  case class Key(val rdd : RDD[Int], key : RDDBlockId) {println("Key count = " + RDDStore.i); RDDStore.i+=1;}

  def putInBlockManager(data: Any): Key = {
    val blockManager = SparkEnv.get.blockManager
    val key = { val x = sc.parallelize(1 to 1); Key(x, RDDBlockId(x.id,0)) }
    blockManager.putIterator(key.key,Array(data).toIterator,StorageLevel.MEMORY_ONLY)
    key
  }

  def getFromBlockManager(key : Key) : Any = {
    val blockManager = SparkEnv.get.blockManager
    blockManager.get(key.key).get.data.next()
  }



  case class GPUMemoryManager(val name : String) {
    var i = 0;
    def getI() = { i+=1; i }
  }

  def blockManagerExtensionDemo(): Unit = {



    val gmm = GPUMemoryManager("Test Manager....")

    val key = putInBlockManager(gmm)

    // scala thread example
    for (i <- 1 to 100) {
      val thread = new Thread {
        override def run {
          val gmm1 = getFromBlockManager(key).asInstanceOf[GPUMemoryManager]
          println("gmm1 i = " + gmm1.getI())
        }
      }
      thread.start
    }

  }


  var sc : SparkContext = _

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    sc = new SparkContext(conf)

    val baseRDD = sc.parallelize(1 to 10, 4).cache()
    baseRDD.setName("baseRDD")

    //check whether its in cachecan
    val blockManager = SparkEnv.get.blockManager
    val key = RDDBlockId(baseRDD.id, 0)

    println("before evaluation " +blockManager.get(key).getOrElse("\nNO DATA"))

    baseRDD.count()

    println("\nAfter runjob\n")
    println(baseRDD.collect.toList)
    for(i <- 0 to 3) {
      val key = RDDBlockId(baseRDD.id, i)
      val x = blockManager.get(key)
      print("DATA IN PARTITION " + (i+1) + " = { ")
      x.get.data.foreach(y => print(y+", "))
      println("}")
    }


    blockManagerExtensionDemo()

  }

}

