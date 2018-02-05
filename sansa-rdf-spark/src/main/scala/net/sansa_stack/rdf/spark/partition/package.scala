package net.sansa_stack.rdf.spark

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import org.apache.spark.sql.Row
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.partition.semantic.RdfPartition

/**
 * Wrap up implicit classes/methods to partition RDF data from N-Triples files into either [[Sparqlify]] or
 * [[Semantic]] partition strategies.
 *
 * @author Gezim Sejdiu
 */

package object partition {

  object Strategy extends Enumeration {
    val CORE, SEMANTIC = Value
  }

  implicit class RDFPartition(rdf: RDD[Triple]) extends Serializable {

    implicit def partitionGraph(strategy: Strategy.Value) = strategy match {
      case Strategy.CORE     => corePartitionGraph()
      case Strategy.SEMANTIC => semanticPartitionGraph()
      case _                 => throw new IllegalArgumentException(s"${strategy} partiton not supported yet!")

    }

    /**
     * Default partition - using VP.
     */
    implicit def corePartitionGraph(): Map[RdfPartitionDefault, RDD[Row]] = {
      RdfPartitionUtilsSpark.partitionGraph(rdf)
    }

    val symbol = Map(
      "space" -> " " * 5,
      "blank" -> " ",
      "tabs" -> "\t",
      "newline" -> "\n",
      "colon" -> ":",
      "comma" -> ",",
      "hash" -> "#",
      "slash" -> "/",
      "question-mark" -> "?",
      "exclamation-mark" -> "!",
      "curly-bracket-left" -> "{",
      "curly-bracket-right" -> "}",
      "round-bracket-left" -> "(",
      "round-bracket-right" -> ")",
      "less-than" -> "<",
      "greater-than" -> ">",
      "at" -> "@",
      "dot" -> ".",
      "dots" -> "...",
      "asterisk" -> "*",
      "up-arrows" -> "^^")
    /**
     * semantic partition of and RDF graph
     */
    implicit def semanticPartitionGraph(): RDD[String] = {
      RdfPartition(symbol, rdf, "", 1).partitionGraph()
    }

  }
}