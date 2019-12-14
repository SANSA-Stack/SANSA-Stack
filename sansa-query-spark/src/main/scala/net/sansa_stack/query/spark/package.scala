package net.sansa_stack.query.spark

import net.sansa_stack.query.spark.datalake.DataLakeEngine
import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.query.spark.ontop.Sparql2Sql
import net.sansa_stack.query.spark.sparqlify.{ QueryExecutionSpark, SparqlifyUtils3 }
import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
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
     * @param sparqlQuery a SPARQL query
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
     * @param sparqlQuery a SPARQL query
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

    /**
     * Semantic partition of and RDF graph
     * @param queryInputPath -- a path to the SPARQL queries.
     */
    def sparql(queryInputPath: String): RDD[String] = {
      new QuerySystem(
        partitions,
        queryInputPath).run()
    }

  }

  implicit class HDT(hdt: DataFrame) extends Serializable {
    import net.sansa_stack.query.spark.hdt._

    /**
      * Querying HDT.
      * @param query a SPARQL query.
      * @return a DataFrame of result set.
      */
    def sparqlHDT(query: String): DataFrame =
     hdt.sparkSession.sql(Sparql2SQL.getQuery(query))

  }

  implicit class Ontop(spark: SparkSession) extends Serializable {
    // val spark = SparkSession.builder().getOrCreate()
    /**
     * Querying through Ontop
     * ->It assumes that the relational tables have already been created
     */
    def sparqlOntop(sparqlFile: String, r2rmlFile: String, owlFile: String, propertyFile: String): DataFrame = {
      var sqlQuery = Sparql2Sql.obtainSQL(sparqlFile, r2rmlFile, owlFile, propertyFile)
      spark.sql(sqlQuery)
    }
  }

  implicit class DataLake(spark: SparkSession) extends Serializable {

    /**
     * Querying a Data Lake.
     */
    def sparqlDL(sparqlQuery: String, mappingsFile: String, configFile: String): DataFrame = {
      DataLakeEngine.run(sparqlQuery, mappingsFile, configFile, spark)
    }
  }
}
