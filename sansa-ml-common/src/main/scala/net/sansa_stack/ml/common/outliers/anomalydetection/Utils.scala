package net.sansa_stack.ml.common.outliers.anomalydetection

import scala.collection.mutable
import scala.collection.mutable.HashSet

import org.apache.jena.graph.Node

object Utils extends Serializable {

  def isNumeric(x: String): Boolean =
    {
      if (x.contains("^")) {
        val c = x.indexOf('^')
        val subject = x.substring(1, c - 1)

        if (isAllDigits(subject)) true
        else false
      } else false
    }

  def isAllDigits(x: String): Boolean = {
    var found = false
    for (ch <- x) {
      if (ch.isDigit || ch == '.') {
        found = true
      } else if (ch.isLetter) {

        found = false
      }
    }

    found
  }

  def searchedge(x: String, y: List[String]): Boolean = {
    if (x.contains("^")) {
      val c = x.indexOf('^')
      val subject = x.substring(c + 2)
      y.contains(subject)
    } else false
  }

  def getLocalName(x: Node): String = {
    var a = x.toString().lastIndexOf("/")
    val b = x.toString().substring(a + 1)
    b
  }
  def searchType(x: String, y: List[String]): Boolean = {
    if (y.exists(x.contains)) {
      true
    } else false
  }

  def getNumber(a: String): Object = {
    val c = a.indexOf('^')
    val subject = a.substring(1, c - 1)

    subject

  }

  def search(a: Double, b: Array[Double]): Boolean = {
    if (b.contains(a)) true
    else false
  }
}

