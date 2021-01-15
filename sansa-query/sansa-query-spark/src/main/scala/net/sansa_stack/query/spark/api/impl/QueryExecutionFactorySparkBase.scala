package net.sansa_stack.query.spark.api.impl

import org.apache.jena.query.QueryFactory
import org.apache.spark.sql.SparkSession

import net.sansa_stack.query.spark.api.domain.{QueryExecutionFactorySpark, QueryExecutionSpark}

/**
 * @author Lorenz Buehmann
 */
abstract class QueryExecutionFactorySparkBase(spark: SparkSession) extends QueryExecutionFactorySpark {
  override def createQueryExecution(query: String): QueryExecutionSpark = createQueryExecution(QueryFactory.create(query))

  override def unwrap[T](aClass: Class[T]): T = null.asInstanceOf[T]

  override def getId: String = "Spark"

  override def getState: String = spark.sessionState.toString

  override def close(): Unit = {}
}
