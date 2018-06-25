package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import net.sansa_stack.rdf.spark.partition.semantic.RdfPartition
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Wrap up implicit classes/methods to partition RDF data from N-Triples
 * files into either [[Sparqlify]] or [[Semantic]] partition strategies.
 *
 * @author Gezim Sejdiu
 */

package object partition {

  object Strategy extends Enumeration {
    val CORE, SEMANTIC = Value
  }

  implicit class RDFPartition(rdf: RDD[Triple]) extends Serializable {

    /**
     * Default partition - using VP.
     */
    def partitionGraph(): Map[RdfPartitionDefault, RDD[Row]] = {
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
    def partitionGraphAsSemantic(): RDD[String] = {
      RdfPartition(symbol, rdf, "", 1).partitionGraph()
    }

  }
}
