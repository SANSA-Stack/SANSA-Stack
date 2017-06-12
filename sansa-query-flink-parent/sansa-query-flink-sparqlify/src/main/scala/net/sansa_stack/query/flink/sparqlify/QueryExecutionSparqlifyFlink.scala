package net.sansa_stack.query.flink.sparqlify

import org.aksw.jena_sparql_api.core.{QueryExecutionBaseSelect, QueryExecutionFactory, ResultSetCloseable}
import org.aksw.jena_sparql_api.utils.ResultSetUtils
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, _}
import org.apache.flink.types.Row
import org.apache.jena.query.{Query, QueryExecution}
import org.apache.jena.sparql.engine.binding.Binding

import scala.collection.JavaConverters._

/**
  * Created by Simon Bin on 12/06/17.
  */
class QueryExecutionSparqlifyFlink( query: Query,
                                    subFactory: QueryExecutionFactory,
                                    val sparqlSqlRewriter: SparqlSqlStringRewriter,
                                    val flinkEnv: ExecutionEnvironment,
                                    val flinkTable: BatchTableEnvironment
                                  ) extends QueryExecutionBaseSelect(query, subFactory) {

  override def executeCoreSelectX(query: Query): QueryExecution = throw new UnsupportedOperationException

  override def executeCoreSelect(query: Query): ResultSetCloseable = {

    val rewrite = sparqlSqlRewriter.rewrite(query)
    val resultVars = rewrite.getProjectionOrder

    val ds = QueryExecutionSparqlifyFlink.createQueryExecution(flinkEnv, flinkTable, rewrite, query)

    val it = ds.collect.iterator

    val tmp = ResultSetUtils.create2(resultVars, it.asJava)
    val result = new ResultSetCloseable(tmp)
    result
  }
}

object QueryExecutionSparqlifyFlink {

  def createQueryExecution(flinkEnv: ExecutionEnvironment, flinkTable: BatchTableEnvironment, rewrite: SparqlSqlStringRewrite, query: Query): DataSet[Binding] = {
    val varDef = rewrite.getVarDefinition.getMap
    val sqlQueryStr = rewrite.getSqlQueryString
    val dataset = flinkTable.sql(sqlQueryStr)
    //		System.out.println("SqlQueryStr: " + sqlQueryStr);
    //		System.out.println("VarDef: " + rewrite.getVarDefinition());
    val rowMapper = new FlinkRowMapperSparqlify(varDef, dataset.getSchema)
    //Function<Row, Binding> fn = x -> rowMapper.apply(x);
    //org.apache.spark.api.java.function.Function<Row, Binding> y = x -> rowMapper.apply(x);
    //val z = JavaKryoSerializationWrapper.wrap(rowMapper)
    val result = dataset.toDataSet[Row].map(rowMapper)
    result
  }
}