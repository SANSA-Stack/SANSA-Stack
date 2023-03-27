package net.sansa_stack.query.spark.api.impl

import net.sansa_stack.query.spark.api.domain.{QueryExecutionFactorySpark, QueryExecutionSpark}
import org.apache.jena.query.QueryFactory
import org.apache.spark.sql.SparkSession

/**
 * A
 * @author Lorenz Buehmann
 */
abstract class QueryExecutionFactorySparkBase(spark: SparkSession, id: Option[String] = None) // FIXME the ID should always be provided
  extends QueryExecutionFactorySpark {

  val DEFAULT_ID = "qef-spark"

  override def createQueryExecution(query: String): QueryExecutionSpark = createQueryExecution(QueryFactory.create(query))

  override def unwrap[T](aClass: Class[T]): T = null.asInstanceOf[T]

  override def getId: String = id.getOrElse(DEFAULT_ID)

  override def getState: String = spark.sessionState.toString

  override def close(): Unit = {}
}
