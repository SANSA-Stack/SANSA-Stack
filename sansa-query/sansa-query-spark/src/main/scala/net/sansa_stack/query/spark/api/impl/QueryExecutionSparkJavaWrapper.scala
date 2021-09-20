package net.sansa_stack.query.spark.api.impl

import net.sansa_stack.query.spark.api.domain.{JavaQueryExecutionSpark, QueryExecutionSpark, ResultSetSpark}
import org.aksw.jena_sparql_api.arq.core.query.QueryExecutionDecoratorBase
import org.apache.jena.sparql.core.{Quad, Var}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD

/**
 * Wrap a JavaQueryExecutionSpark with the scala interface
 *
 * @param decoratee The JavaQueryExecutionSpark instance to wrap
 */
class QueryExecutionSparkJavaWrapper(decoratee: JavaQueryExecutionSpark)
  extends QueryExecutionDecoratorBase[JavaQueryExecutionSpark](decoratee)
    with QueryExecutionSpark
{
  override def execSelectSpark(): ResultSetSpark = {
    val javaRs = decoratee.execSelectSparkJava

    new ResultSetSpark {
      import collection.JavaConverters._
      override def getResultVars: Seq[Var] = javaRs.getResultVars.asScala.toSeq
      override def getBindings: RDD[Binding] = javaRs.getRdd.rdd
    }
  }

  override def execConstructSpark: RDD[org.apache.jena.graph.Triple] = decoratee.execConstructSparkJava.rdd

  override def execConstructQuadsSpark: RDD[Quad] = decoratee.execConstructQuadsSparkJava().rdd
}
