package net.sansa_stack.query.spark.ontop

import org.apache.jena.rdf.model.Model
import org.apache.spark.sql.SparkSession

import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark
import net.sansa_stack.query.spark.api.impl.QueryEngineFactoryBase

/**
 * @author Lorenz Buehmann
 */
class QueryEngineFactoryOntop(spark: SparkSession) extends QueryEngineFactoryBase(spark) {

  override def create(database: String, mappingModel: Model): QueryExecutionFactorySpark = {
    val ontop: OntopSPARQLEngine = null

    new QueryExecutionFactorySparkOntop(spark, ontop)
  }
}
