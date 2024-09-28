package net.sansa_stack.query.spark.sparqlify

import org.aksw.jenax.arq.util.binding.ResultSetUtils
import org.aksw.jenax.arq.util.exec.query.QueryExecutionAdapter
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite
import org.apache.jena.query.{Query, ResultSetCloseable}
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

    val tmp = ResultSetUtils.createUsingVars(resultVars, it)
    new ResultSetCloseable(tmp, new QueryExecutionAdapter)
  }

}
