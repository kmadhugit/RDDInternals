package org.madhu

/**
 * Created by kmadhu on 25/2/16.
 */

class A {
  def functA(): Unit = {
    println("funcA")
  }
}

class B(objA : A) {
  def functB(): Unit = {
    println("funcB")
  }
}
object X {
  implicit class C(objA: A) {
    def functC(): Unit = {
      println("funcC")
    }
  }

}

object implicitTest {

  implicit def converFunc(objA : A) = new B(objA)

  import X._

  def main(args : Array[String]): Unit = {
    val A = new A()
    A.functB();
    A.functC();

  }

}
