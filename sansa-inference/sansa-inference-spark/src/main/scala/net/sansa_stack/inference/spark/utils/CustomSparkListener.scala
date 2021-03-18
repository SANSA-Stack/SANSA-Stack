package net.sansa_stack.inference.spark.utils

import org.apache.spark.scheduler._

/**
  * @author Lorenz Buehmann
  */
class CustomSparkListener extends SparkListener{

  var startTime: Long = 0
  var name: String = ""

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    name = applicationStart.appName
    startTime = applicationStart.time
    println(s"$name started...")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println(s"$name finished in ${applicationEnd.time-startTime}ms")
  }
}
