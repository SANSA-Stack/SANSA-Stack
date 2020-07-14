package net.sansa_stack.query.spark.ontop


import org.aksw.jena_sparql_api.core.QueryExecutionFactoryBackQuery
import org.apache.jena.query.{Query, QueryExecution}
import org.apache.spark.sql.SparkSession;

class QueryExecutionFactoryOntopSpark(spark: SparkSession, ontop: OntopSPARQLEngine)
  extends QueryExecutionFactoryBackQuery {

  override def getId: String = "spark"

  override def getState: String = spark.toString

  override def createQueryExecution(query: Query): QueryExecution = new QueryExecutionOntopSpark(query, this, ontop)
}
