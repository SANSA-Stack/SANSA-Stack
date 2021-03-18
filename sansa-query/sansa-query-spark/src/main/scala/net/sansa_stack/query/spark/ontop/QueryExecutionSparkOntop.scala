package net.sansa_stack.query.spark.ontop

import org.aksw.jena_sparql_api.core.{QueryExecutionFactory, ResultSetCloseable}
import org.apache.jena.query.{Query, ResultSet}
import org.apache.spark.sql.SparkSession

import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.api.impl.{QueryExecutionSparkBase, ResultSetSparkImpl}
import scala.collection.JavaConverters._

import org.aksw.jena_sparql_api.utils.ResultSetUtils

class QueryExecutionSparkOntop(query: Query,
                               subFactory: QueryExecutionFactory,
                               spark: SparkSession,
                               ontop: QueryEngineOntop)
  extends QueryExecutionSparkBase(query, subFactory, spark) {

  override def execSelectSpark(): ResultSetSpark = {
    val bindings = ontop.computeBindings(query.toString())

    val resultVars = query.getProjectVars.asScala

    new ResultSetSparkImpl(resultVars, bindings)
  }

  override def executeCoreSelect(query: Query): ResultSetCloseable = {
    if (ontop.settings.useLocalEvaluation) {
      val bindings = ontop.computeBindingsLocal(query.toString).iterator

      val rs = ResultSetUtils.create2(query.getProjectVars, bindings.asJava)

      new ResultSetCloseable(rs)
    } else {
      super.executeCoreSelect(query)
    }
  }
}
