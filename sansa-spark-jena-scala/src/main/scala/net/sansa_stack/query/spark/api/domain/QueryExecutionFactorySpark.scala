package net.sansa_stack.query.spark.api.domain

import org.aksw.jenax.dataaccess.sparql.factory.execution.query.QueryExecutionFactory
import org.apache.jena.query.Query

/**
 * Place to make [[QueryExecutionSpark]] objects from [[Query]] objects or a string.
 *
 * @author Lorenz Buehmann
 */
trait QueryExecutionFactorySpark extends QueryExecutionFactory {
  override def createQueryExecution(query: Query): QueryExecutionSpark
  override def createQueryExecution(query: String): QueryExecutionSpark
}
