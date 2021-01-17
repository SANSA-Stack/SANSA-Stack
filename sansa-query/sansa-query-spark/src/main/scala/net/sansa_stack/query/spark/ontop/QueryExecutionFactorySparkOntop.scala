package net.sansa_stack.query.spark.ontop

import org.apache.jena.query.{Query, QueryExecutionFactory}
import org.apache.spark.sql.SparkSession

import net.sansa_stack.query.spark.api.domain.QueryExecutionSpark
import net.sansa_stack.query.spark.api.impl.QueryExecutionFactorySparkBase

/**
 * A query execution factory for Ontop.
 *
 * @author Lorenz Buehmann
 */
class QueryExecutionFactorySparkOntop(spark: SparkSession,
                                      ontop: OntopSPARQLEngine)
  extends QueryExecutionFactorySparkBase(spark) {

  override def createQueryExecution(query: Query): QueryExecutionSpark = {
    new QueryExecutionSparkOntop(query, this, spark, ontop)
  }
}
