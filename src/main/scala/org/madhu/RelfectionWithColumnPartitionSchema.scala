package org.madhu

/**
 * Created by kmadhu on 4/11/15.
 */
object RelfectionWithColumnPartitionSchema {

  case class Person ( val name : Array[Double], val age : Int)

  def main(args : Array[String]) = {
    val schema = ColumnPartitionSchema.schemaFor[Person]
    // println(schema._columns.foreach(x => println(x.prettyAccessor)))
  }

}
