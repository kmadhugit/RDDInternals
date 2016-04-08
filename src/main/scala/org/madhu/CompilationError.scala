package org.madhu
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext, TaskContext}
/**
  * Created by madhusudanan on 08/04/16.
  */
object CompilationError {

  case class Rating(user : String)

  def topkFilms(new_User:RDD[Rating],userFeatureData:RDD[(Int,Array[Double])],productFeatureData:RDD[(Int,Array[Double])],k: Int) = {
    val user =new_User.map(x => (x.user)).collect()
    val newUserFeatures=userFeatureData.filter(x => user.contains(x._1)).map(x =>(x._2)).collect()
    val Features = breeze.linalg.DenseVector(newUserFeatures(0))
    val topFilms = productFeatureData.map{case (item,features) =>
      (item, (breeze.linalg.DenseVector(features) dot Features))}.takeOrdered(5)(Ordering[Double].reverse.on(x=>x._2))
    val kRecommendations = List(user.mkString, topFilms.mkString)
  }


  def main(args : Array[String]): Unit = {
    println("Hello")
  }

}
