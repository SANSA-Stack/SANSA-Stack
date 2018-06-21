package net.sansa_stack.query.spark

import scala.collection.JavaConverters._

import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.query.spark.sparqlify.{ QueryExecutionSpark, SparqlifyUtils3 }
import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import org.aksw.jena_sparql_api.core.ResultSetCloseable
import org.aksw.jena_sparql_api.utils.ResultSetUtils
import org.apache.jena.graph.Triple
import org.apache.jena.query.QueryFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }

/**
 * Wrap up implicit classes/methods to query RDF data from N-Triples files into either [[Sparqlify]] or
 * [[Semantic]] partition strategies.
 *
 * @author Gezim Sejdiu
 */

package object query {

  implicit class SparqlifyAsDefault(triples: RDD[Triple]) extends Serializable {
    import net.sansa_stack.rdf.spark.partition._

    val spark = SparkSession.builder().getOrCreate()
    /**
     * Default partition - using VP.
     */
    def sparql(sparqlQuery: String): DataFrame = {
      val partitions = triples.partitionGraph()

      val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)
      val query = QueryFactory.create(sparqlQuery)
      val rewrite = rewriter.rewrite(query)
      val df = QueryExecutionSpark.createQueryExecution(spark, rewrite, query)

      df
    }

  }

  implicit class Sparqlify(partitions: Map[RdfPartitionDefault, RDD[Row]]) extends Serializable {

    val spark = SparkSession.builder().getOrCreate()
    /**
     * Default partition - using VP.
     */
    def sparql(sparqlQuery: String): DataFrame = {
      val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)
      val query = QueryFactory.create(sparqlQuery)
      val rewrite = rewriter.rewrite(query)
      val df = QueryExecutionSpark.createQueryExecution(spark, rewrite, query)
      /**
       * val it = rdd.toLocalIterator.asJava
       * val resultVars = rewrite.getProjectionOrder()
       *
       * val tmp = ResultSetUtils.create2(resultVars, it)
       * new ResultSetCloseable(tmp)
       */
      df
    }

  }

  implicit class Semantic(partitions: RDD[String]) extends Serializable {

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
    def sparql(sparqlQuery: String)(input: String, output: String, numOfFilesPartition: Int): Unit = {
      new QuerySystem(symbol, partitions,
        input,
        output,
        numOfFilesPartition).run()
    }

  }
}
