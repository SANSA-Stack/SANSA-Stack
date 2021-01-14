package net.sansa_stack.query.spark.ontop

import java.io.{ByteArrayOutputStream, File, FileInputStream}

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import net.sansa_stack.query.spark.query._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Triple
import org.apache.jena.query.{Query, QueryFactory, ResultSet, ResultSetFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.engine.ResultSetStream
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.resultset.ResultSetCompare
import org.apache.jena.sys.JenaSystem
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import scala.collection.JavaConverters._
import scala.io.Source

import org.aksw.sparqlify.core.sql.common.serialization.{SqlEscaperBase, SqlEscaperDoubleQuote}

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitionerComplex}
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.spark.partition.core.{RdfPartitionUtilsSpark, SQLUtils, SparkTableGenerator}

class OntopTests extends FunSuite with DataFrameSuiteBase {

  var triples: RDD[Triple] = _
  var sparqlExecutor: QueryExecutor = _

  var ontop: QueryEngineOntop = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    JenaSystem.init();

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath

    triples = spark.rdf(Lang.NTRIPLES)(input).cache()

    val mappingsModel = ModelFactory.createDefaultModel()
    val partitioner = new RdfPartitionerComplex()
    val partitions2RDD = RdfPartitionUtilsSpark.partitionGraph(triples, partitioner)
    val partitions = partitions2RDD.keySet.toSeq

    val tableNameFn: RdfPartitionStateDefault => String = p => SQLUtils.escapeTablename(R2rmlUtils.createDefaultTableName(p))
    SparkTableGenerator(spark).createAndRegisterSparkTables(partitioner,
      partitions2RDD,
      extractTableName = tableNameFn)
    R2rmlUtils.createR2rmlMappings(partitioner, partitions, tableNameFn, new SqlEscaperDoubleQuote(), mappingsModel, true)
    mappingsModel.write(System.out, "Turtle")
    ontop = QueryEngineOntop(spark, "test", mappingsModel, None)

//    sparqlExecutor = new OntopSPARQLExecutor(triples)
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

  val queries = List("Q1", "Q2", "Q3")

  queries.foreach(q => {
    test(s"Test Ontop with BSBM $q") {
      val queryString = Source.fromFile(getClass.getResource(s"/sparklify/queries/bsbm/$q.sparql").getPath).getLines.mkString("\n")
      val query = QueryFactory.create(queryString)

      val result = ontop.execSelect(queryString).collect()
//      val result = sparqlExecutor.sparqlRDD(queryString).collect()

      val rs = resultSetFromBindings(query, result)

      val rsTarget = ResultSetFactory.fromXML(new FileInputStream(new File(getClass.getResource(s"/sparklify/queries/bsbm/$q.srx").getPath)))

      assert(resultSetEquivalent(query, rs, rsTarget))
    }
  })

  private def resultSetFromBindings(query: Query, bindings: Array[Binding]): ResultSet = {
    val model = ModelFactory.createDefaultModel()
    val rs = new ResultSetStream(query.getResultVars, model, bindings.toList.asJava.iterator())
    rs
  }

  def resultSetEquivalent(query: Query, resultsActual: ResultSet, resultsExpected: ResultSet): Boolean = {
    val testByValue = true

    if (testByValue) {
      if (query.isOrdered) {
        ResultSetCompare.equalsByValueAndOrder(resultsExpected, resultsActual)
      } else {
        ResultSetCompare.equalsByValue(resultsExpected, resultsActual)
      }
    } else {
      if (query.isOrdered) {
        ResultSetCompare.equalsByTermAndOrder(resultsExpected, resultsActual)
      } else {
        ResultSetCompare.equalsByTerm(resultsExpected, resultsActual)
      }
    }
  }
}
