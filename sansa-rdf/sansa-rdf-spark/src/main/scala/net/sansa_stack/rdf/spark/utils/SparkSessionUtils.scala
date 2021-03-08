package net.sansa_stack.rdf.spark.utils

import org.aksw.commons.sql.codec.util.SqlCodecUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionUtils {
  /** A seemingly clean way to unambiguously obtain a SparkSession from a given RDD */
  def getSessionFromRdd(rdd: RDD[_]): SparkSession = {
    SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
  }

  /**
   * perform some SQL query processing taking queries from a CLI
   *
   * @param spark       the Spark session
   * @param df          the [[DataFrame]] to work on
   * @param stopKeyword the keyword to stop the input loop
   */
  def sqlQueryHook(spark: SparkSession, df: DataFrame, stopKeyword: String = "q"): Unit = {
    df.show(false)

    var input = ""
    while (input != stopKeyword) {
      println("enter SQL query (press 'q' to quit): ")
      input = scala.io.StdIn.readLine()
      try {
        spark.sql(input).show(false)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  val sqlEscaper = SqlCodecUtils.createSqlCodecForApacheSpark()

  def clearAllTablesAndViews(spark: SparkSession): Unit = {
    spark.catalog.listDatabases().collect().foreach(db => {
      spark.catalog.listTables(db.name).collect().foreach(t => {
        val b = spark.catalog.dropTempView(s"${sqlEscaper.forSchemaName().encode(t.name)}")
      })
    })
  }

  /**
   * Returns the current active/registered executors excluding the driver.
   *
   * @param sc The Spark context to retrieve registered executors.
   * @return a list of executors each in the form of host:port.
   */
  def currentActiveExecutors(sc: SparkContext): Seq[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.map(_._1)
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(!_.split(":")(0).equals(driverHost)).toList
  }

  /**
   * Returns the current active/registered executors excluding the driver.
   *
   * @param spark The Spark session to retrieve registered executors.
   * @return a list of executors each in the form of host:port.
   */
  def currentActiveExecutors(spark: SparkSession): Seq[String] = {
    val allExecutors = spark.sparkContext.getExecutorMemoryStatus.map(_._1)
    val driverHost: String = spark.sparkContext.getConf.get("spark.driver.host")
    allExecutors.filter(!_.split(":")(0).equals(driverHost)).toList
  }
}
