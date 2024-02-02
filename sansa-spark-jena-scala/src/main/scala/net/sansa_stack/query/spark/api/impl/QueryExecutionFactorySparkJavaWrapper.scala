package net.sansa_stack.query.spark.api.impl

import net.sansa_stack.query.spark.api.domain.{JavaQueryExecutionFactorySpark, QueryExecutionFactorySpark, QueryExecutionSpark}
import org.aksw.jenax.dataaccess.sparql.factory.execution.query.QueryExecutionFactoryWrapperBase
import org.apache.jena.query.Query

/**
 * Scala wrapper for a [[JavaQueryExecutionFactorySpark]]
 *
 * @author Claus Stadler
 * @param delegate
 */
class QueryExecutionFactorySparkJavaWrapper(delegate: JavaQueryExecutionFactorySpark)
  extends QueryExecutionFactoryWrapperBase[JavaQueryExecutionFactorySpark](delegate)
  with QueryExecutionFactorySpark
{
  override def createQueryExecution(query: Query): QueryExecutionSpark = new QueryExecutionSparkJavaWrapper(decoratee.createQueryExecution(query))
  override def createQueryExecution(queryString: String): QueryExecutionSpark = new QueryExecutionSparkJavaWrapper(decoratee.createQueryExecution(queryString))
}
