package net.sansa_stack.query.spark.api.impl

import net.sansa_stack.query.spark.api.domain.{JavaQueryExecutionFactorySpark, QueryExecutionFactorySpark, QueryExecutionSpark}
import org.aksw.jena_sparql_api.transform.QueryExecutionFactoryDecoratorBase
import org.apache.jena.query.Query

/**
 * Scala wrapper for a [[JavaQueryExecutionFactorySpark]]
 *
 * @author Claus Stadler
 * @param delegate
 */
class QueryExecutionFactorySparkJavaWrapper(delegate: JavaQueryExecutionFactorySpark)
  extends QueryExecutionFactoryDecoratorBase[JavaQueryExecutionFactorySpark](delegate)
  with QueryExecutionFactorySpark
{
  override def createQueryExecution(query: Query): QueryExecutionSpark = new QueryExecutionSparkJavaWrapper(decoratee.createQueryExecution(query))
  override def createQueryExecution(queryString: String): QueryExecutionSpark = new QueryExecutionSparkJavaWrapper(decoratee.createQueryExecution(queryString))
}
