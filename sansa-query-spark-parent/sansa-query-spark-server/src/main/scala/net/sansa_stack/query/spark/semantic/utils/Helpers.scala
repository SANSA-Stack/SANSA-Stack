package net.sansa_stack.query.spark.semantic.utils


import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.TimeUnit

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

    // total query process time
    def queryTime(processedTime: Long,symbol: Map[String, String]) = {
        val milliseconds = TimeUnit.MILLISECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
        val seconds = Math.floor(milliseconds/1000d + .5d).toInt
        val minutes = TimeUnit.MINUTES.convert(processedTime, TimeUnit.NANOSECONDS)

        if (milliseconds >= 0) {
            println("Processed Time (MILLISECONDS): " + milliseconds)

            if (seconds > 0) {
                println("Processed Time (SECONDS): " + seconds + " approx.")

                if (minutes > 0) {
                    println("Processed Time (MINUTES): " + minutes)
                }
            }
        }

        println(symbol("newline"))
        // append query time
        milliseconds
        //_queriesProcessTime.append(milliseconds)

        
    }

    // overall queries process time
    def overallQueriesTime(_queriesProcessTime: ArrayBuffer[Long]): Unit = {
        val milliseconds: Long = _queriesProcessTime.sum
        val seconds = Math.floor(milliseconds/1000d + .5d).toInt

        if (milliseconds >= 1000) {
            println("--> Overall Process Time: " + milliseconds + "ms (" + seconds + "secs approx.)")
        } else {
            println("--> Overall Process Time: " + milliseconds + "ms")
        }
    }
}