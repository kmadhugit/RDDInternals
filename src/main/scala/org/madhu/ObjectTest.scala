package org.madhu

/**
 * Created by kmadhu on 23/2/16.
 */
object ObjectTest {

  object GPUSparkEnv {

    println("Object created")

    val i = 10;

  }

  def main(args : Array[String]): Unit = {

    println("in main");
    println(GPUSparkEnv.i);
    println(GPUSparkEnv.i);
    println(GPUSparkEnv.i);


  }

}
