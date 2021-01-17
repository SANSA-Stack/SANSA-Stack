package net.sansa_stack.query.spark.api.impl

import org.apache.spark.sql.SparkSession

import net.sansa_stack.query.spark.api.domain.QueryEngineFactory

/**
 * @author Lorenz Buehmann
 */
abstract class QueryEngineFactoryBase(spark: SparkSession) extends QueryEngineFactory {

}
