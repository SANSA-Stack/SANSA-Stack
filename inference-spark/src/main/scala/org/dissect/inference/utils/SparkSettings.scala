package org.dissect.inference.utils

import org.apache.spark.storage.StorageLevel

/**
  * @author Lorenz Buehmann
  */
object SparkSettings {

  /**
    * Gives the storage level for every RDD that will be cached.
    * Can be useful for later uses if the storage level can be changed
    * if one is set
    */
  final val STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_ONLY

  /**
    * Executor memory
    */
  var executorMem: String = null

  /**
    * HDFS path for the result
    */
  var outputFilePath: String = null

  /**
    * Memory fraction which is used for caching
    */
  var memoryFraction: String = null

  /**
    * Default parallelism
    */
  var parallelism: String = null

  /**
    * Flag if it should run in locale mode
    */
  var locale: Boolean = false

  /**
    * Job name
    */
  var jobName: String = null


}
