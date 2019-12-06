package net.sansa_stack.datalake.spark

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by mmami on 26.01.17.
  */
object Main extends App {

    var queryFile = args(0)
    val mappingsFile = args(1)
    val configFile = args(2)
    val executorID = args(3)

    val spark = SparkSession.builder.master(executorID).appName("Sparkall").getOrCreate

    val hadoopConfig = spark.conf

    val executor : SparkExecutor = new SparkExecutor(spark, mappingsFile)

    val stopwatch: StopWatch = new StopWatch
    stopwatch.start()

    val run = new Run[DataFrame](executor)
    run.application(queryFile, mappingsFile, configFile)

    stopwatch.stop()

    val timeTaken = stopwatch.getTime

    println(s"Query execution time: $timeTaken ms")

}
