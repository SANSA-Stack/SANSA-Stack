package net.sansa_stack.rdf.spark.partition.core

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import org.apache.jena.graph.NodeFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

import net.sansa_stack.rdf.common.partition.core.RdfPartitionStateDefault

/**
 * Some utilities for working with SQL objects.
 *
 * @author Lorenz Buehmann
 */
object SQLUtils {

  def escapeTablename(name: String, quoted: Boolean = true, quotChar: Char = '"'): String = {
    val s = URLEncoder.encode(name, StandardCharsets.UTF_8.toString)
      .toLowerCase
      .replace('%', 'P')
      .replace('.', 'C')
      .replace("-", "dash")
    if (quoted) s"""$quotChar$s$quotChar""" else s
  }

  /**
   * perform some SQL query processing taking queries from a CLI
   * @param spark
   * @param df
   * @param stopKeyword
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

}
