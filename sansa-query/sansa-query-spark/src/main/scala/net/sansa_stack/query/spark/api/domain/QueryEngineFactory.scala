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

  def create(database: String, mappingModel: Model): QueryExecutionFactorySpark

}
