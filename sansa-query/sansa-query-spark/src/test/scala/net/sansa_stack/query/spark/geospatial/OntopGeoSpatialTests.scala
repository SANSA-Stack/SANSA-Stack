package net.sansa_stack.query.spark.geospatial

import java.io.{File, FileInputStream}

import scala.collection.JavaConverters._
import scala.io.Source

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.graph.Triple
import org.apache.jena.query.{Query, QueryFactory, ResultSet, ResultSetFactory, ResultSetFormatter}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.engine.ResultSetStream
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.resultset.ResultSetCompare
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import net.sansa_stack.query.spark.query._
import net.sansa_stack.rdf.spark.io._

class OntopGeoSpatialTests extends FunSuite with DataFrameSuiteBase {

  var triples: RDD[Triple] = _
  var sparqlExecutor: QueryExecutor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val input = getClass.getResource("/geospatial/geospatial-data.nt").getPath

    triples = spark.rdf(Lang.NTRIPLES)(input)

    sparqlExecutor = new OntopSPARQLExecutor(triples)
  }

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    conf
  }

  test(s"Test GeoSPARQL support") {
    val queryString = Source.fromFile(getClass.getResource(s"/geospatial/geospatial.sparql").getPath).getLines.mkString("\n")
    val query = QueryFactory.create(queryString)

    val result = sparqlExecutor.sparqlRDD(queryString).collect()

    val rs = resultSetFromBindings(query, result)

    ResultSetFormatter.out(rs)
  }


  private def resultSetFromBindings(query: Query, bindings: Array[Binding]): ResultSet = {
    val model = ModelFactory.createDefaultModel()
    val rs = new ResultSetStream(query.getResultVars, model, bindings.toList.asJava.iterator())
    rs
  }

}
