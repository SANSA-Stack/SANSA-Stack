package net.sansa_stack.query.spark.sparqlify

import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark
import net.sansa_stack.query.spark.api.impl.{QueryEngineFactoryBase, QueryExecutionFactorySparkJavaWrapper}
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.apache.jena.rdf.model.Model
import org.apache.spark.sql.SparkSession

class QueryEngineFactorySparqlify(sparkSession: SparkSession) extends QueryEngineFactoryBase(sparkSession) {
  override def create(databaseName: String, mappingModel: Model): QueryExecutionFactorySpark = {
    val rewriter: SparqlSqlStringRewriter = SparqlifyUtils3.createSparqlSqlRewriter(sparkSession, databaseName, mappingModel)

    val result = new QueryExecutionFactorySparkJavaWrapper(
      new JavaQueryExecutionFactorySparqlifySpark(sparkSession, rewriter))

    result
  }
}
