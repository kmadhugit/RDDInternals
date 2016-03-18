package org.madhu


/**
 * Created by kmadhu on 29/10/15.
 */

case class abc(val i : Int,val  j : Float, val k: Int, val l : Float) {

  override def toString = "(" + i + ", " + j + ", " + k + ", " + l + ")"
}

class gpuPGM {

  def program(inp1 : Array[Int], inp2 : Array[Float], op1 : Array[Int], op2 : Array[Float]) = {
    for (i <- 0 to inp1.length - 1) {
      op1(i) = op1(i) + inp1(i)
      op2(i) = op2(i) + inp2(i)
    }
  }
}

object lamdaExtraction {

  class RDD[T,U](arr : Array[T]) {

    def launchKernel[V,X](gpu: gpuPGM, lamdaIP: (T => X), lamdaOP : ( T, V) => U)   = {
      //extract 2 arrays and create 2 un initalized arrays for this example, Number of inputArray and number of Output Array will changed
      // based on V and X. V will give inpur Array details ; X will give output array details.
    }

  }


  def main(args : Array[String]) = {

    val gpu = new gpuPGM()
    val arr = (1 to 10).map(x => new abc(x,x*3,0,0)).toArray
    val r = new RDD[abc,abc](arr)

    // CPU PROCESSING

    println(arr.toList)
    val r1 = arr.map(x => new abc(x.i,x.j,x.k+x.i,x.l+x.j) )
    println(r1.toList)

    // GPU PROCESSING

    def l1(x : abc) = (x.i,x.j)
    def l2(x : abc,r : (Int,Float)) = new abc(x.i,x.j,r._1,r._2)

    val r2 = r.launchKernel(gpu,l1,l2)

    // Implement launchKernel in a way r1 == r2
    // Do not modify function protypes without discussing with me.

  }

}
