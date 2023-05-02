package net.sansa_stack.query.spark.semantic.utils

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

/*
 * Helplers
 */
object Helpers {

  /*
   * fetchTriplesSPO - fetch SUBJECT, PREDICATE and OBJECT
   */
  def fetchTripleSPO(triple: String, symbol: Map[String, String]): ArrayBuffer[String] = {
    // return list
    val tripleData: ArrayBuffer[String] = ArrayBuffer()

    // fetch indices
    val locationPoint1 = triple.indexOf(symbol("blank"))
    val locationPoint2 = triple.lastIndexOf(symbol("blank"))

    // WHERE clause: SUBJECT, PREDICATE and OBJECT
    val tripleSubject = triple.substring(0, locationPoint1).trim()
    val triplePredicate = triple.substring(locationPoint1, locationPoint2).trim()
    val tripleObject = triple.substring(locationPoint2, triple.length()).trim()

    // append data
    tripleData.append(tripleSubject)
    tripleData.append(triplePredicate)
    tripleData.append(tripleObject)

    tripleData
  }

  // fetch FILTER function data
  def fetchFilterFunctionData(fName: String, filterFunction: String, processLine: String, symbol: Map[String, String]): ArrayBuffer[String] = {
    val data: ArrayBuffer[String] = ArrayBuffer()

    // split
    val splitData = filterFunction.split(fName)
    var locationPoint = splitData(1).indexOf(symbol("round-bracket-right"))

    // variable (?X)
    val variable = splitData(1).substring(1, locationPoint)

    // value (<...>)
    locationPoint = processLine.indexOf(variable)
    var value = processLine.substring(locationPoint + variable.length + 1)
    locationPoint = value.indexOf(symbol("blank"))
    if (locationPoint.equals(-1)) value = value
    else value = value.substring(0, locationPoint)

    // append data
    data.append(variable)
    data.append(value)

    data
  }

  // total query process time
  def queryTime(processedTime: Long, symbol: Map[String, String]): Long = {
    val milliseconds = TimeUnit.MILLISECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
    val seconds = Math.floor(milliseconds / 1000d + .5d).toInt
    val minutes = TimeUnit.MINUTES.convert(processedTime, TimeUnit.NANOSECONDS)

    if (milliseconds >= 0) {
      println(s"Processed Time (MILLISECONDS): $milliseconds")

      if (seconds > 0) {
        println(s"Processed Time (SECONDS): $seconds approx.")

        if (minutes > 0) {
          println(s"Processed Time (MINUTES): $minutes")
        }
      }
    }
    println(symbol("newline"))
    // append query time
    milliseconds
  }

  // overall queries process time
  def overallQueriesTime(_queriesProcessTime: ArrayBuffer[Long]): Unit = {
    val milliseconds: Long = _queriesProcessTime.sum
    val seconds = Math.floor(milliseconds / 1000d + .5d).toInt

    if (milliseconds >= 1000) {
      println(s"--> Overall Process Time: ${milliseconds}ms (${seconds}secs approx.)")
    } else {
      println(s"--> Overall Process Time: ${milliseconds}ms")
    }
  }
}
