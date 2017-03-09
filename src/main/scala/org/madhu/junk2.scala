package org.madhu

import java.io.{File, PrintWriter}

import scala.collection.mutable.HashMap
import scala.io.Source



/**
 * Created by kmadhu on 19/1/16.
 */


object junk2 {

  case class record(seq : String, oper : String, operand : String, result: String,
                    allocSeq : String, parentSeq : String, parentAllocSeq : String)

  // Between : and (
  def getOperator(inp : String) = {
    inp.split(":")(1).split("\\(")(0)
  }

  def getOperand(inp : String) = {
    inp.split("\\(")(1).split("\\)")(0)
  }

  def getReturn(inp : String) = {
   "0x" + inp.split("=")(1).trim().split("\t")(0)
  }

  def hextoInt(inp : String): Unit = {
    Integer.parseInt(inp.split("x")(1),16)
  }

  def main(args: Array[String]) {

    var map = scala.collection.mutable.Map[String, (String,String)]()
    val records = scala.collection.mutable.ArrayBuffer.empty[record]

    val seq = (0 to Int.MaxValue-1).iterator
    var allocSeq = (0 to Int.MaxValue-1).iterator

    val lines = Source.fromFile("./truss.out").getLines()

    // val totalLine = lines.length
    var cnt = 0;
    var line = ""

    def moveToNextLine() {
      var l = "";

      if(lines.hasNext)
        l = lines.next();

      cnt+=1;
      println("cnt = " + cnt + " line = " + l )

      // if(l.equals(line)) { cnt+=1; l=lines.next();}
    }

    while (lines.hasNext) {

      moveToNextLine();

      val oper = getOperator(line)

      if(oper.equals("malloc")) {
        val operand = getOperand(line)
        moveToNextLine();
        val ret = getReturn(line);
        val rSeq = seq.next().toString;
        val aSeq = allocSeq.next().toString
        records += new record(rSeq,"M", operand, ret, aSeq,"-1","-1")
        map += (ret -> (rSeq,aSeq))
      }
      else if(oper.equals("realloc")) {
        val operands = getOperand(line).split(",").map(_.trim)
        val addr = operands(0)
        val parentSeq = map.getOrElse(addr,("-1","-1"))
        val operand = operands(1)
        moveToNextLine();
        val ret = getReturn(line);
        val rSeq = seq.next().toString;
        val aSeq = allocSeq.next().toString
        if(parentSeq._1.equals("-1")) {
          println("unable to find mapping for realloc, keeping that as malloc " + line)
          records += new record(rSeq, "M", operand, ret, aSeq, parentSeq._1, parentSeq._2)
        }
        else {
          records += new record(rSeq, "R", operand, ret, aSeq, parentSeq._1, parentSeq._2)
          map.remove(addr)
        }
        map += (ret ->(rSeq, aSeq))
      }
      else if(oper.equals("free")) {
        val operand = getOperand(line)
        val parentSeq = map.getOrElse(operand,("-1","-1"))

        if(parentSeq._1.equals("-1")) {
          println("unable to find mapping for FREE " + line)
        }
        else {
          map.remove(operand)
          val rSeq = seq.next().toString
          records += new record(rSeq, "F", "-1", "-1", "-1", parentSeq._1, parentSeq._2)
        }
        //ignore next line
        moveToNextLine();
      }

    }

    val pw = new PrintWriter(new File("/tmp/input.csv" ))
    pw.write(allocSeq.next() + "\n")
    def convertDec(inp : String) = {
      if(inp.equals("-1"))
        "-1"
      else {
        Integer.parseInt(inp.split("x")(1),16)
      }

    }

    records.foreach( x => {
      val op = x.allocSeq + "~" +  x.oper + "~" + convertDec(x.operand) + "~" + x.parentAllocSeq + "\n";
      pw.write(op)
    })
    pw.close()
  }
}


