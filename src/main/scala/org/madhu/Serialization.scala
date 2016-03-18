package org.madhu

import java.io.{ObjectOutputStream, ObjectInputStream}
import java.io.{FileOutputStream, FileInputStream}

/**
 * Created by kmadhu on 21/9/15.
 */

class Person(val name:String, age: Int) extends Serializable {
  override def toString = s"Person is $name age = $age"
}
object Serialization {

  def main(args :Array[String]) = {

    val os = new ObjectOutputStream(new FileOutputStream("/tmp/example.dat"))
    os.writeObject(new Person("Madhu", 22))
    os.close()

    val is = new ObjectInputStream(new FileInputStream("/tmp/example.dat"))
    val obj = is.readObject()
    is.close()

    println(obj)
  }

}
