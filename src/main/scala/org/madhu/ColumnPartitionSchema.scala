/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.madhu

import java.io.{ObjectInputStream, ObjectOutputStream}

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{TermSymbol, Type, typeOf}
import java.io.IOException
import scala.util.control.NonFatal

// Some code taken from org.apache.spark.sql.catalyst.ScalaReflection

object Utils {

  def tryOrIOException(block: => Unit) {

    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }

  /**
   * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
   * exceptions as IOException. This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
   */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }
}

object ColumnPartitionSchema {

  // Since we are creating a runtime mirror usign the class loader of current thread,
  // we need to use def at here. So, every time we call mirror, it is using the
  // class loader of the current thread.
  // TODO check out bug https://issues.scala-lang.org/browse/SI-6240 about reflection not being
  // thread-safe before 2.11 http://docs.scala-lang.org/overviews/reflection/thread-safety.html
  def mirror: universe.Mirror =
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)

  var onlyLoadableClassesSupported: Boolean = false

  def schemaFor[T: ClassTag]: ColumnPartitionSchema = {

    def columnsFor(tpe: Type): IndexedSeq[ColumnSchema] = {
      tpe match {
        // 8-bit signed BE
        case t if t <:< typeOf[Byte] => Vector(new ColumnSchema(BYTE_COLUMN))
        // 16-bit signed BE
        case t if t <:< typeOf[Short] => Vector(new ColumnSchema(SHORT_COLUMN))
        // 32-bit signed BE
        case t if t <:< typeOf[Int] => { println("int from tpe "); Thread.dumpStack(); Vector(new ColumnSchema(INT_COLUMN)) }
        // 64-bit signed BE
        case t if t <:< typeOf[Long] => Vector(new ColumnSchema(LONG_COLUMN))
        // 32-bit single-precision IEEE 754 floating point
        case t if t <:< typeOf[Float] => Vector(new ColumnSchema(FLOAT_COLUMN))
        // 64-bit double-precision IEEE 754 floating point
        case t if t <:< typeOf[Double] => Vector(new ColumnSchema(DOUBLE_COLUMN))
        // TODO boolean - it does not have specified size
        // TODO char
        // TODO string - along with special storage space
        // TODO array (especially constant size)
        // TODO option
        // TODO protection from cycles
        // TODO caching schemas for classes
        // TODO make it work with nested classes
        // TODO objects that contains null object property
        // TODO objects with internal objects without vals/vars will end up with null fields here
        // Generic object
        case t => {

          val valVarMembers = t.members.view
            .filter(p => !p.isMethod && p.isTerm).map(_.asTerm)
            .filter(p => p.isVar || p.isVal)

          valVarMembers.foreach { p =>
            // TODO more checks
            // is final okay?
            if (p.isStatic) throw new UnsupportedOperationException(
              s"Column schema with static field ${p.fullName} not supported")
          }

          valVarMembers.flatMap { term =>
            columnsFor(term.typeSignature).map { schema =>
              new ColumnSchema(
                schema.columnType,
                term +: schema.terms)
            }
          } .toIndexedSeq
        }
        /*        case other =>
                  throw new UnsupportedOperationException(s"Column schema for type $other not supported")*/
      }
    }

    val runtimeCls = implicitly[ClassTag[T]].runtimeClass

    val columns = runtimeCls match {
      // special case for primitives, since their type signature shows up as AnyVal instead
      case c if c == classOf[Byte] => Vector(new ColumnSchema(BYTE_COLUMN))
      case c if c == classOf[Short] => Vector(new ColumnSchema(SHORT_COLUMN))
      case c if c == classOf[Int] => { println("int from runtimeClass "); Vector(new ColumnSchema(INT_COLUMN)) }
      case c if c == classOf[Long] => Vector(new ColumnSchema(LONG_COLUMN))
      case c if c == classOf[Float] => Vector(new ColumnSchema(FLOAT_COLUMN))
      case c if c == classOf[Double] => Vector(new ColumnSchema(DOUBLE_COLUMN))
      // generic case for other objects
      case _ =>
        val clsSymbol = mirror.classSymbol(runtimeCls)
        columnsFor(clsSymbol.typeSignature)
    }

    new ColumnPartitionSchema(columns.toArray, runtimeCls)
  }

}

/**
 * A schema of a ColumnPartitionData. columns contains information about columns and cls is the
 * class of the serialized type, unless it is primitive - then it is null.
 */
class ColumnPartitionSchema(
                              var _columns: Array[ColumnSchema],
                              var _cls: Class[_]) extends Serializable {

  def columns: Array[ColumnSchema] = _columns

  def cls: Class[_] = _cls

  /**
   * Whether the schema is for a primitive value.
   */
  def isPrimitive: Boolean = columns.size == 1 && columns(0).terms.isEmpty

  /**
   * Amount of bytes used for storage of specified number of records using this schema.
   */
  def memoryUsage(size: Long): Long = {
    columns.map(_.memoryUsage(size)).sum
  }

  /**
   * Returns column schemas ordered by given pretty accessor names.
   */
  def orderedColumns(order: Seq[String]): Seq[ColumnSchema] = {
    val columnsByAccessors = HashMap(columns.map(col => col.prettyAccessor -> col): _*)
    order.map(columnsByAccessors(_))
  }

  def getters: Array[Any => Any] = {
    val mirror = ColumnPartitionSchema.mirror
    columns.map { col =>
      col.terms.foldLeft(identity[Any] _)((r, term) =>
        ((obj: Any) => mirror.reflect(obj).reflectField(term).get) compose r)
    }
  }

  // the first argument is object, the second is value
  def setters: Array[(Any, Any) => Unit] = {
    assert(!isPrimitive)
    val mirror = ColumnPartitionSchema.mirror
    columns.map { col =>
      val getOuter = col.terms.dropRight(1).foldLeft(identity[Any] _)((r, term) =>
        ((obj: Any) => mirror.reflect(obj).reflectField(term).get) compose r)

      (obj: Any, value: Any) =>
        mirror.reflect(getOuter(obj)).reflectField(col.terms.last).set(value)
    }
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.writeObject(_columns)
    if (!isPrimitive) {
      out.writeUTF(_cls.getName())
    }
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    _columns = in.readObject().asInstanceOf[Array[ColumnSchema]]
    if (!isPrimitive) {
      _cls = Class.forName(in.readUTF())
    }
  }

}

/**
 * A column is one basic property (primitive, String, etc.).
 */
class ColumnSchema(
                    /** Type of the property. Is null when the whole object is a primitive. */
                    private var _columnType: ColumnType,
                    /** Scala terms with property name and other information */
                    private var _terms: Vector[TermSymbol] = Vector[TermSymbol]()) extends Serializable {

  def columnType: ColumnType = _columnType

  def terms: Vector[TermSymbol] = _terms

  /**
   * Chain of properties accessed starting from the original object. The first tuple argument is
   * the full name of the class containing the property and the second is property's name.
   */
  def propertyChain: Vector[(String, String)] = {
    val mirror = ColumnPartitionSchema.mirror
    terms.map { term =>
      (mirror.runtimeClass(term.owner.asClass).getName, term.name.toString)
    }
  }

  def prettyAccessor: String = {
    val mirror = ColumnPartitionSchema.mirror
    "this" + terms.map("." + _.name.toString.trim).mkString
  }

  def memoryUsage(size: Long): Long = {
    columnType.bytes * size
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // TODO make it handle generic owner objects by passing full type information somehow
    out.writeObject(_columnType)
    out.writeObject(propertyChain)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val mirror = ColumnPartitionSchema.mirror
    _columnType = in.readObject().asInstanceOf[ColumnType]
    _terms =
      in.readObject().asInstanceOf[Vector[(String, String)]].map { case (clsName, propName) =>
        val cls = Class.forName(clsName)
        val typeSig = mirror.classSymbol(cls).typeSignature
        typeSig.declaration(universe.stringToTermName(propName)).asTerm
      }
  }

}

abstract class ColumnType {
  /** How many bytes does a single property take. */
  val bytes: Int
}

case object BYTE_COLUMN extends ColumnType {
  val bytes = 1
}

case object SHORT_COLUMN extends ColumnType {
  val bytes = 2
}

case object INT_COLUMN extends ColumnType {
  val bytes = 4
}

case object LONG_COLUMN extends ColumnType {
  val bytes = 8
}

case object FLOAT_COLUMN extends ColumnType {
  val bytes = 4
}

case object DOUBLE_COLUMN extends ColumnType {
  val bytes = 8
}
