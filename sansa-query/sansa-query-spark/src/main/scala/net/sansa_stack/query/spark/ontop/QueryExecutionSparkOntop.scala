package net.sansa_stack.query.spark.ontop

import org.aksw.jena_sparql_api.core.QueryExecutionFactory
import org.apache.jena.query.Query
import org.apache.spark.sql.SparkSession

import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.api.impl.{QueryExecutionSparkBase, ResultSetSparkImpl}

import scala.collection.JavaConverters._

class QueryExecutionSparkOntop(query: Query,
                               subFactory: QueryExecutionFactory,
                               spark: SparkSession,
                               ontop: OntopSPARQLEngine)
    extends QueryExecutionSparkBase(query, subFactory, spark) {

  override def execSelectSpark(): ResultSetSpark = {
    val bindings = ontop.execSelect(query.toString())

    val resultVars = query.getProjectVars.asScala

    new ResultSetSparkImpl(resultVars, bindings)
  }

}
