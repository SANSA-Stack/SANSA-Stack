package net.sansa_stack.query.spark.api.domain

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
   * Creates a query engine factory on a pre-partitioned dataset. The RDF data is distributed among Spark
   * tables located either in the given database or the default database. Mappings from triples to tables is
   * provided by the mapping model.
   *
   * @param database the database that holds the tables for the RDF data, if `null` the default database will be used
   * @param mappingModel the model containing the mappings
   * @return a query execution factory
   */
  def create(database: String, mappingModel: Model): QueryExecutionFactorySpark

  /**
   * Creates a query engine factory for the given RDD of triples.
   *
   * @param triples the RDD of triples
   * @return a query execution factory
   */
  def create(triples: RDD[Triple]): QueryExecutionFactorySpark

}
