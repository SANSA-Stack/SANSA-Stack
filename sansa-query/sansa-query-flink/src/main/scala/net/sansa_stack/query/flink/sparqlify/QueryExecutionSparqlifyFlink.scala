package net.sansa_stack.query.flink.sparqlify

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

import com.esotericsoftware.kryo.io.{Input, Output}
import org.aksw.jena_sparql_api.core.{QueryExecutionBaseSelect, ResultSetCloseable}
import org.aksw.jenax.arq.connection.core.QueryExecutionFactory
import org.aksw.jenax.arq.util.binding.ResultSetUtils
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, _}
import org.apache.flink.types.Row
import org.apache.jena.query.{Query, QueryExecution}
import org.apache.jena.sparql.engine.binding.Binding

import scala.collection.JavaConverters._

/**
  * Created by Simon Bin on 12/06/17.
  */
class QueryExecutionSparqlifyFlink(
                                    query: Query,
                                    subFactory: QueryExecutionFactory,
                                    val sparqlSqlRewriter: SparqlSqlStringRewriter,
                                    val flinkEnv: ExecutionEnvironment,
                                    val flinkTable: BatchTableEnvironment)
  extends QueryExecutionBaseSelect(query, subFactory) {

  override def executeCoreSelectX(query: Query): QueryExecution = throw new UnsupportedOperationException

  override def executeCoreSelect(query: Query): ResultSetCloseable = {

    val rewrite = sparqlSqlRewriter.rewrite(query)
    val resultVars = rewrite.getProjectionOrder

    val ds = QueryExecutionSparqlifyFlink.createQueryExecution(flinkEnv, flinkTable, rewrite, query)

    val it = ds.collect.iterator

    val tmp = ResultSetUtils.createUsingVars(resultVars, it.asJava)
    val result = new ResultSetCloseable(tmp)
    result
  }

  // override def setInitialBinding(binding: Binding): Unit = throw new UnsupportedOperationException

  override def getTimeout1(): Long = -1

  override def execJson(): org.apache.jena.atlas.json.JsonArray = throw new UnsupportedOperationException

  override def execJsonItems(): java.util.Iterator[org.apache.jena.atlas.json.JsonObject] = throw new UnsupportedOperationException

}

object QueryExecutionSparqlifyFlink {

  def createQueryExecution(flinkEnv: ExecutionEnvironment, flinkTable: BatchTableEnvironment, rewrite: SparqlSqlStringRewrite, query: Query): DataSet[Binding] = {
    val varDef = rewrite.getVarDefinition.getMap
    val sqlQueryStr = rewrite.getSqlQueryString.replace("SELECT true WHERE FALSE", "SELECT true FROM `http://ex.org/empty_table` WHERE false")

    println("SQL Query: " + sqlQueryStr)

    val dataset = flinkTable.sqlQuery(sqlQueryStr)
    // System.out.println("SqlQueryStr: " + sqlQueryStr);
    // System.out.println("VarDef: " + rewrite.getVarDefinition());
    val rowMapper = new FlinkRowMapperSparqlify(varDef, dataset.getSchema.getFieldNames)
    val config = flinkEnv.getConfig
    // Function<Row, Binding> fn = x -> rowMapper.apply(x);
    // org.apache.spark.api.java.function.Function<Row, Binding> y = x -> rowMapper.apply(x);
    val kryo = new KryoSerializer[FlinkRowMapperSparqlify](classOf[FlinkRowMapperSparqlify], config).getKryo

    val byteStream = new ByteArrayOutputStream()
    val kryoOut = new Output(byteStream)
    kryo.writeClassAndObject(kryoOut, rowMapper)
    kryoOut.close()
    byteStream.flush()
    val bytes = byteStream.toByteArray
    println("byte size=" + bytes.length)
    println(util.Arrays.toString(bytes))
    dataset.printSchema()

    implicit val typeInfo = TypeInformation.of(classOf[Row])
    val result = dataset.toDataSet[Row].map(row => {
      val kryo = new KryoSerializer[FlinkRowMapperSparqlify](classOf[FlinkRowMapperSparqlify], config).getKryo

      println("byte size=" + bytes.length)
      val input = new Input(new ByteArrayInputStream(bytes))
      val rowMapper2 = kryo.readClassAndObject(input).asInstanceOf[FlinkRowMapperSparqlify]
      input.close()
      println(rowMapper2)
      println(row)
      val result = rowMapper2.map(row)
      println(result)
      result
    })
    result
  }
}
