package net.sansa_stack.query.spark.api.domain

import net.sansa_stack.rdf.common.partition.core.RdfPartitioner
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.Model
import org.apache.spark.rdd.RDD

/**
 * Creates a query engine represented by a query execution factory.
 *
 * @author Lorenz Buehmann
 */
trait QueryEngineFactory {

  /**
   * Get the currently set partitioner associated with this engine.
   * The partitioner may change between calls to [[create()]].
   *
   * Experimental.
   *
   * @return The RDF partitioner
   */
  def getPartitioner: RdfPartitioner[_]

  /**
   * Creates a query engine factory on a pre-partitioned dataset. The RDF data is distributed among Spark
   * tables located either in the given database or the default database. Mappings from triples to tables is
   * provided by the mapping model.
   *
   * @param database     the database that holds the tables for the RDF data, if no database has been provided the
   *                     default database will be used
   * @param mappingModel the model containing the mappings
   * @return a query execution factory
   */
  def create(database: Option[String], mappingModel: Model): QueryExecutionFactorySpark

  def create(database: String, mappingModel: Model): QueryExecutionFactorySpark =
    create(Option(database), mappingModel)

  /**
   * Creates a query engine factory for the given RDD of triples.
   * A Spark database named by the ID of the given triples RDD will be used to maintain the tables used during
   * SPARQL-to-SQL rewriting and execution.
   *
   * @param triples the RDD of triples
   * @return a query execution factory
   */
  def create(triples: RDD[Triple]): QueryExecutionFactorySpark

}
