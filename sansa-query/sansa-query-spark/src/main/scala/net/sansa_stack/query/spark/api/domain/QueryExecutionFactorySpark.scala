package net.sansa_stack.query.spark.api.domain

import org.aksw.jena_sparql_api.core.QueryExecutionFactory
import org.apache.jena.query.Query

/**
 * @author Lorenz Buehmann
 */
trait QueryExecutionFactorySpark extends QueryExecutionFactory {
  override def createQueryExecution(query: Query): QueryExecutionSpark
  override def createQueryExecution(query: String): QueryExecutionSpark
}
