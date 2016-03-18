package org.madhu

import scala.reflect.runtime.{universe=>ru}
import scala.reflect.runtime.currentMirror
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


/**
 * Created by kmadhu on 21/9/15.
 */

case class hybridIterator[T : ClassTag](arr : Array[T]) extends Iterator[T]
{
  def this(y : Iterator[T]) = this(y.toArray)

  var idx : Int = -1


  val runtimeCls = implicitly[ClassTag[T]].runtimeClass
  val clsSymbol = currentMirror.classSymbol(runtimeCls)
  val valVarMembers = clsSymbol.typeSignature.members.view .filter(p => !p.isMethod && p.isTerm).map(_.asTerm)
    .filter(p => p.isVar || p.isVal)

  var symbolNames = valVarMembers.map(term => term.name.toString).toList

  var symbolTypes = valVarMembers.map {
    term => term.typeSignature match {
      case c if (c == typeOf[Int]) => "Int"
      case c if (c == typeOf[Array[Int]]) => "Array[Int]"
      case _ => "unknown"
    }
  }.toList


  println(" symbols = " + symbolNames + " types " + symbolTypes)

  // columnsFor(clsSymbol.typeSignature)

  if(valVarMembers.isEmpty) {
    symbolNames = List("this")
    runtimeCls match {
      // special case for primitives, since their type signature shows up as AnyVal instead
      case c if c == classOf[Int] => { println("TYPE => Int "); }
      case c if c == classOf[Array[Int]] => { println("TYPE => Array[Int]")}
    }
  }


  def getColumn(cname : String)  = {
    if (cname.equals("this")) {
     arr
    }
    else {
      val symbol = valVarMembers.find(_.name.toString.startsWith(cname)).get
      def get(obj: Any) = currentMirror.reflect(obj).reflectField(symbol).get
      arr.map(x => get(x))
    }
  }

  def hasNext = idx < arr.length - 1

  def next = {
    idx = idx + 1
    arr(idx)
  }

}


case class Point(columnX: Int, columnY: Array[Int]) { override def toString() = "x = " + columnX + " y = " + columnY }

object reflection {


  def main(args : Array[String]) = {

    val ptArray = (1 to 10).map(x => new Point(x,Array(1,2))).toArray


    val ht = hybridIterator(ptArray)

    val c = ht.getColumn("columnY")

    println("c length " + c.length)

    val x = c.flatMap(c=> c.asInstanceOf[Array[Int]])

    println(x.toList)

    println("#########")




      def instantiateClass( cls: Class[_], enclosingObject: AnyRef): AnyRef = {

      // Use reflection to instantiate object without calling constructor
      val rf = sun.reflect.ReflectionFactory.getReflectionFactory()
      val parentCtor = classOf[java.lang.Object].getDeclaredConstructor()
      val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
      val obj = newCtor.newInstance().asInstanceOf[AnyRef]

      if (enclosingObject != null) {
        val field = cls.getDeclaredField("$outer")
        field.setAccessible(true)
        field.set(obj, enclosingObject)
      }
      obj
    }

/*    val pt = new Point(10,20)
    val mirror = currentMirror
    val typeSig = mirror.classSymbol(implicitly[ClassTag[Point]].runtimeClass).typeSignature
    //m == TermSymbol
    val symbol = typeSig.members.filter(_.isTerm).map(_.asTerm).filter(_.isVal).find(_.name.toString.startsWith("columnY")).get

    val mirrorField = mirror.reflect(pt).reflectField(symbol)

    println(mirrorField.get)
    mirrorField.set(30)

    println(pt)

    println(symbol.name)*/
  }


}
