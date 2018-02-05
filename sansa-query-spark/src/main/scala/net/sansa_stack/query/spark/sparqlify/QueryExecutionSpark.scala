package net.sansa_stack.query.spark.sparqlify

import org.aksw.jena_sparql_api.views.RestrictedExpr
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite
import org.apache.jena.query.Query
import org.aksw.jena_sparql_api.utils.ResultSetUtils
import org.aksw.jena_sparql_api.core.ResultSetCloseable
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._

import com.google.common.collect.Multimap
import java.util.Iterator
import scala.collection.JavaConverters._

import net.sansa_stack.rdf.spark.utils.kryo.io.JavaKryoSerializationWrapper
import net.sansa_stack.rdf.spark.utils.kryo.io.KryoSerializationWrapper

object QueryExecutionSpark {

  def createQueryExecution(spark: SparkSession, rewrite: SparqlSqlStringRewrite, query: Query) = {

    val varDef = rewrite.getVarDefinition().getMap()

    val sqlQueryStr = rewrite.getSqlQueryString()
    val dataset = spark.sql(sqlQueryStr)

//    System.out.println("SqlQueryStr: " + sqlQueryStr);
//    System.out.println("VarDef: " + rewrite.getVarDefinition())

    val rowMapper = new SparkRowMapperSparqlify(varDef)

    val z = JavaKryoSerializationWrapper.wrap(rowMapper)

    dataset.javaRDD.map(z).rdd
  }

  def ResultSet(rdd: RDD[Binding], rewrite: SparqlSqlStringRewrite) = {
    val it = rdd.toLocalIterator.asJava
    val resultVars = rewrite.getProjectionOrder()

    val tmp = ResultSetUtils.create2(resultVars, it)
    new ResultSetCloseable(tmp)
  }

}