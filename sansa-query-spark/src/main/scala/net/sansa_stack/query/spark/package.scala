package net.sansa_stack.query.spark

import org.apache.jena.graph.Triple
import org.apache.jena.query.QueryFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import net.sansa_stack.query.spark.datalake.DataLakeEngine
import net.sansa_stack.query.spark.ontop.OntopSPARQLEngine
import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.query.spark.sparqlify.{QueryExecutionSpark, SparqlifyUtils3}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionDefault, RdfPartitionerComplex}
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark

/**
 * Wrap up implicit classes/methods to query RDF data from N-Triples files into either [[Sparqlify]] or
 * [[Semantic]] partition strategies.
 *
 * @author Gezim Sejdiu
 */

package object query {

  object SPARQLEngine extends Enumeration {
    type SPARQLEngine = Value
    val Ontop, Sparqlify = Value
  }

  /**
   * A SPARQL executor backed by a SPARQL engine.
   *
   * @param triples the triples to work on
   * @param engine the SPARQL query engine
   */
  class SPARQLExecutor(triples: RDD[Triple], engine: SPARQLEngine.Value = SPARQLEngine.Sparqlify)
    extends QueryExecutor
      with Serializable {

    private val queryExecutor = {
      engine match {
        case SPARQLEngine.Ontop => new OntopSPARQLExecutor(triples)
        case SPARQLEngine.Sparqlify => new SparqlifySPARQLExecutor(triples)
      }
    }

    override def sparql(sparqlQuery: String): DataFrame = {
      queryExecutor.sparql(sparqlQuery)
    }
  }

  trait QueryExecutor {
    /**
     * Execute a SPARQL query and return a Spark DataFrame.
     *
     * Default partitioning scheme used is VP.
     *
     * @param sparqlQuery a SPARQL query
     */
    def sparql(sparqlQuery: String): DataFrame
  }

  /**
   * An Ontop backed SPARQL executor implicitly bound to an RDD[Triple].
   *
   * VP is used as partitioning strategy, i.e. one partition s,o per predicate p in the RDF dataset.
   *
   * @param triples the triples to work on
   */
  implicit class OntopSPARQLExecutorAsDefault(triples: RDD[Triple])
    extends QueryExecutor
      with Serializable {

    val queryExecutor = new SPARQLExecutor(triples, engine = SPARQLEngine.Ontop)

    override def sparql(sparqlQuery: String): DataFrame = {
      queryExecutor.sparql(sparqlQuery)
    }
  }

  /**
   * An Ontop backed SPARQL executor working on the given RDF partitions.
   *
   * @param partitions the RDF partitions to work on
   */
  class OntopSPARQLExecutor(partitions: Map[RdfPartitionComplex, RDD[Row]])
    extends QueryExecutor
      with Serializable {

    def this(triples: RDD[Triple]) {
      this(RdfPartitionUtilsSpark.partitionGraph(triples, partitioner = RdfPartitionerComplex()))
    }

    val spark = SparkSession.builder().getOrCreate()

    val sparqlEngine = new OntopSPARQLEngine(spark, partitions)

    /**
     * Default partition - using VP.
     *
     * @param sparqlQuery a SPARQL query
     */
    override def sparql(sparqlQuery: String): DataFrame = {
      sparqlEngine.execute(sparqlQuery)
    }
  }

  /**
   * A Sparqlify backed SPARQL executor working on the given RDF partitions.
   *
   * @param partitions the RDF partitions to work on
   */
  class SparqlifySPARQLExecutor(var partitions: Map[RdfPartitionDefault, RDD[Row]])
    extends QueryExecutor
      with Serializable {

    /**
     * A Sparqlify backed SPARQL executor working on the given RDF triples.
     *
     * VP is used as partitioning strategy, i.e. one partition s,o per predicate p in the RDF dataset.
     *
     * @param triples the triples to work on
     */
    def this(triples: RDD[Triple]) {
      this(RdfPartitionUtilsSpark.partitionGraph(triples))
    }

    val spark = SparkSession.builder().getOrCreate()

    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)

    override def sparql(sparqlQuery: String): DataFrame = {
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
     *
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
     *
     * @param query a SPARQL query.
     * @return a DataFrame of result set.
     */
    def sparqlHDT(query: String): DataFrame =
      hdt.sparkSession.sql(Sparql2SQL.getQuery(query))

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
