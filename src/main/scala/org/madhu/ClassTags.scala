package org.madhu

import scala.collection.mutable.{HashMap, Map}
import scala.reflect.ClassTag

/**
 * Created by kmadhu on 28/10/15.
 */

object cudaManager {

  private val registeredKernels  = new HashMap[String, CUDAKernel[_,_]]()

  /**
   * Register given CUDA kernel in the manager.
   */
  def registerCUDAKernel(name: String, kernel: CUDAKernel[_,_]) {
    if (registeredKernels.contains(name)) {
      println("It contains")
    }

    synchronized {
      registeredKernels.put(name, kernel)
    }
  }

  /**
   * Gets the kernel registered with given name. Must not be called when `registerCUDAKernel` might
   * be called at the same time from another thread.
   */
  def getKernel(name: String): Option[CUDAKernel[_,_]] = {
    registeredKernels.get(name)

  }

}

class rdd [T,U]{

  def mapGPU(kname : String, x : T) = {
    println("mapGPU called")
    val kernel = cudaManager.getKernel("k1").get.asInstanceOf[CUDAKernel[T,U]]
    kernel.fun(x)
  }
}

abstract class CUDAKernel[T,U]{
  def fun (x : T ) : U
}

object ClassTags {

  // There can be multiple versions of myCUDAKernel with different values for T,U.
  class myCUDAKernel extends CUDAKernel[Int,Int] {
    override def fun(x : Int): Int = 10
  }

  def main(args : Array[String])  {

    val kernel = new myCUDAKernel()
    cudaManager.registerCUDAKernel("k1",kernel)
    println(new rdd().mapGPU("k1",10))
  }

}
