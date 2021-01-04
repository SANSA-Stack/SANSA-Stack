package net.sansa_stack.query.spark.sparqlify

import org.aksw.jena_sparql_api.core.ResultSetCloseable
import org.aksw.jena_sparql_api.utils.ResultSetUtils
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite
import org.apache.jena.query.Query
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

object QueryExecutionSpark {

  def createQueryExecution(spark: SparkSession, rewrite: SparqlSqlStringRewrite, query: Query): DataFrame = {

    val varDef = rewrite.getVarDefinition().getMap()

    val sqlQueryStr = rewrite.getSqlQueryString()
    val df = spark.sql(sqlQueryStr)

    /*
    System.out.println("SqlQueryStr: " + sqlQueryStr);
    System.out.println("VarDef: " + rewrite.getVarDefinition())

    val rowMapper = new SparkRowMapperSparqlify(varDef)

    val z = JavaKryoSerializationWrapper.wrap(rowMapper)

    //dataset.javaRDD.map(z).rdd
     */
    df
  }

  def ResultSet(rdd: RDD[Binding], rewrite: SparqlSqlStringRewrite): ResultSetCloseable = {
    val it = rdd.toLocalIterator.asJava
    val resultVars = rewrite.getProjectionOrder()

    val tmp = ResultSetUtils.create2(resultVars, it)
    new ResultSetCloseable(tmp)
  }

}
