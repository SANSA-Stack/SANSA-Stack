package org.dissect.inference.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Lorenz Buehmann
  */
object SparkManager {

  /**
    * Spark context
    */
  private var context: SparkContext = null

  /**
    * Creates a new Spark context
    *
    * @see https://spark.apache.org/docs/1.6.1/configuration.html
    */
  def createSparkContext() {
    val conf = new SparkConf()

    // Use the Kryo serializer, because it is faster than Java serializing
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "de.tf.uni.freiburg.sparkrdf.sparql.serialization.Registrator")
    conf.set("spark.core.connection.ack.wait.timeout", "5000");
    conf.set("spark.shuffle.consolidateFiles", "true");
    conf.set("spark.rdd.compress", "true");
    conf.set("spark.kryoserializer.buffer.max.mb", "512");

    if (SparkSettings.locale) {
      conf.setMaster("local")
    }

    if (SparkSettings.executorMem != null) {
      conf.set("spark.executor.memory", SparkSettings.executorMem)
    }

    if (SparkSettings.parallelism != null) {
      conf.set("spark.default.parallelism", SparkSettings.parallelism)
    }

    if (SparkSettings.memoryFraction != null) {
      conf.set("spark.storage.memoryFraction", SparkSettings.memoryFraction)
    }

    if (SparkSettings.jobName != null) {
      conf.setAppName(SparkSettings.jobName)
    }

    context = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
  }

  /**
    * Close the Spark context
    */
  def closeContext() {
    context.stop()
  }
}
