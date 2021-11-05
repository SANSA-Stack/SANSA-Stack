package net.sansa_stack.query.spark.ontop

import net.sansa_stack.query.spark.api.domain.QueryExecutionSpark
import net.sansa_stack.query.spark.api.impl.QueryExecutionFactorySparkBase
import org.apache.jena.query.Query
import org.apache.spark.sql.SparkSession

/**
 * A query execution factory for Ontop.
 *
 * @author Lorenz Buehmann
 */
class QueryExecutionFactorySparkOntop(spark: SparkSession,
                                      id: Option[String],
                                      val ontop: QueryEngineOntop)
  extends QueryExecutionFactorySparkBase(spark, id) {

  override def createQueryExecution(query: Query): QueryExecutionSpark = {
    new QueryExecutionSparkOntop(query, this, spark, ontop)
  }
}
