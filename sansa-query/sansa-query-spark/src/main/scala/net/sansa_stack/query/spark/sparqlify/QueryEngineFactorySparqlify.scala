package net.sansa_stack.query.spark.sparqlify

import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark
import net.sansa_stack.query.spark.api.impl.QueryExecutionFactorySparkJavaWrapper
import net.sansa_stack.query.spark.impl.QueryEngineFactoryBase
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.apache.jena.rdf.model.Model
import org.apache.spark.sql.SparkSession

class QueryEngineFactorySparqlify(sparkSession: SparkSession)
  extends QueryEngineFactoryBase(sparkSession, RdfPartitionerDefault) {
  override def create(databaseName: Option[String], mappingModel: Model): QueryExecutionFactorySpark = {
    val rewriter: SparqlSqlStringRewriter = SparqlifyUtils3.createSparqlSqlRewriter(sparkSession, databaseName, mappingModel)

    val result = new QueryExecutionFactorySparkJavaWrapper(
      new JavaQueryExecutionFactorySparqlifySpark(sparkSession, rewriter))

    result
  }
}
