package net.sansa_stack.query.spark.ontop

import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.api.impl.{QueryExecutionSparkBase, ResultSetSparkImpl}
import org.aksw.jenax.arq.util.binding.ResultSetUtils
import org.aksw.jenax.dataaccess.sparql.factory.execution.query.QueryExecutionFactory
import org.apache.jena.query.{Query, ResultSetCloseable}
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

class QueryExecutionSparkOntop(query: Query,
                               subFactory: QueryExecutionFactory,
                               spark: SparkSession,
                               ontop: QueryEngineOntop)
  extends QueryExecutionSparkBase(query, subFactory, spark) {

  override def execSelectSpark(): ResultSetSpark = {
    val bindings = ontop.computeBindings(query.toString())

    val resultVars = query.getProjectVars.asScala

    new ResultSetSparkImpl(resultVars.toList, bindings)
  }

  override def executeCoreSelect(query: Query): ResultSetCloseable = {
    if (ontop.settings.useLocalEvaluation) {
      val bindings = ontop.computeBindingsLocal(query.toString).iterator

      val rs = ResultSetUtils.createUsingVars(query.getProjectVars, bindings.asJava)

      new ResultSetCloseable(rs, this)
    } else {
      super.executeCoreSelect(query)
    }
  }
}
