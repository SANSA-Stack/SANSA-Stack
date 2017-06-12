package net.sansa_stack.query.flink.sparqlify

import net.sansa_stack.rdf.partition.schema.SchemaStringString
import org.aksw.jena_sparql_api.core.{QueryExecutionBaseSelect, QueryExecutionFactory, ResultSetCloseable}
import org.apache.jena.query.{Query, QueryExecution}
import org.aksw.jena_sparql_api.core.ResultSetCloseable
import org.aksw.jena_sparql_api.utils.ResultSetUtils
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.jena.query.ResultSet
import org.apache.flink.api.scala._
import org.apache.flink.util._
import org.apache.flink.table.api.scala._

/**
  * Created by Simon Bin on 12/06/17.
  */
class QueryExecutionSparqlifyFlink( val query: Query,
                                    val subFactory: QueryExecutionFactory,
                                    val sparqlSqlRewriter: SparqlSqlStringRewriter,
                                    val flinkEnv: ExecutionEnvironment,
                                    val flinkTable: BatchTableEnvironment
                                  ) extends QueryExecutionBaseSelect(query, subFactory) {

  override def executeCoreSelectX(query: Query): QueryExecution = throw new UnsupportedOperationException

  override def executeCoreSelect(query: Query): ResultSetCloseable = {

    val rewrite = sparqlSqlRewriter.rewrite(query)
    val resultVars = rewrite.getProjectionOrder

    val ds = QueryExecutionUtilsFlink.createQueryExecution(flinkEnv, flinkTable, rewrite, query)

    val it = ds.collect.iterator

    val tmp = ResultSetUtils.create2(resultVars, it)
    val result = new ResultSetCloseable(tmp)
  }
}
