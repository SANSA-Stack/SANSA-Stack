package net.sansa_stack.query.spark.sparqlify

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.apache.jena.query.{ARQ, QueryFactory, ResultSetFormatter}
import org.apache.jena.riot.{Lang, RIOT}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.FunSuite

import scala.io.Source

class SparklifyQueryEngineTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.query.spark.query._

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.eventLog.enabled", "true")
      .set("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
    conf
  }

  test("result of running BSBM Q1 should match") {

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val query = Source.fromFile(getClass.getResource("/sparklify/queries/bsbm/Q1.sparql").getPath).getLines.mkString

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val result = triples.sparql(query)

    val size = result.count()

    assert(size == 7)
  }

  test("result of running BSBM Q2 should match") {

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val query = Source.fromFile(getClass.getResource("/sparklify/queries/bsbm/Q2.sparql").getPath).getLines.mkString

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val result = triples.sparql(query)

    val size = result.count()

    assert(size == 4)
  }

  test("result of running BSBM Q3 should match") {

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val query = Source.fromFile(getClass.getResource("/sparklify/queries/bsbm/Q3.sparql").getPath).getLines.mkString

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val result = triples.sparql(query)

    val size = result.count()

    assert(size == 674)
  }

  // TODO Separate issue-related queries from BSBM

  /*
  // Disabled test because Sparqlify at present does not handle longs and decimals correctly
  test("result of running issue14 should match") {

    val input = getClass.getResource("/datasets/issue14.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    assert(triples.sparql("SELECT * { ?s ?p ?o FILTER(?o > 900000000000000000) }").count() == 1)
  }
  */

  test("result of running issue15 should match") {

    val input = getClass.getResource("/datasets/issue43.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    assert(triples.sparql("SELECT * { ?s ?p ?o FILTER(REGEX(STR(?o), 'Clare')) }").count() == 1)
    assert(triples.sparql("SELECT * { ?s ?p ?o FILTER(REGEX(STR(?o), 'clare', 'i')) }").count() == 1)

    // Counter checks
    assert(triples.sparql("SELECT * { ?s ?p ?o FILTER(REGEX(STR(?o), 'clare')) }").count() == 0)
    assert(triples.sparql("SELECT * { ?s ?p ?o FILTER(REGEX(STR(?o), 'foobar')) }").count() == 0)
  }

  test("result of running issue17 (strafter) should match") {

    val input = getClass.getResource("/datasets/issue43.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    assert(triples.sparql("SELECT * { ?s ?p ?o FILTER(STRAFTER(?o, 'Cl') = 'are')}").count() == 1)
    assert(triples.sparql("SELECT * { ?s ?p ?o FILTER(STRAFTER(?o, 'Cl') = 'foo')}").count() == 0)
    assert(triples.sparql("SELECT * { ?s ?p ?o FILTER(STRSTARTS(?o, 'Cl'))}").count() == 1)
    assert(triples.sparql("SELECT * { ?s ?p ?o FILTER(STRENDS(?o, 'are'))}").count() == 1)
  }

  test("result of running issue34 should match") {

    val input = getClass.getResource("/datasets/issue34.nt").getPath
    val query = "SELECT * { ?s ?p ?o }"

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val result = triples.sparql(query)

    val size = result.count()

    assert(size == 1)
  }

  // Due to a likely catalyst bug, this test so far does not succeed
  /*
  test("result of running issue35 should match") {

    val input = getClass.getResource("/datasets/issue43.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    assert(triples.sparql("SELECT DISTINCT ?s ?o { ?s <http://xmlns.com/foaf/0.1/name> ?o } ORDER BY ?o").count() == 1)
  }
  */

  // FIXME The result set of 43 has incorrect bnode labels
  // The issue may be a bug in Sparqlify, but it may as well be
  // simply an artifact of line-based ntriples processing
  test("result of running issue43 should match") {

    val input = getClass.getResource("/datasets/issue43.nt").getPath
    val queryStr = "SELECT * { ?s ?p ?o }"

    ARQ.enableBlankNodeResultLabels()

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val partitions: Map[RdfPartitionDefault, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(triples)
    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)
    val qef = new QueryExecutionFactorySparqlifySpark(spark, rewriter)
    val str = ResultSetFormatter.asText(qef.createQueryExecution(queryStr).execSelect())
        .toLowerCase
    assert(!str.contains("null"))
  }

  test("result of running issue44 should match") {

    val input = getClass.getResource("/datasets/issue44.nt").getPath
    val query = Source.fromFile(getClass.getResource("/sparklify/queries/bsbm/issue44.sparql").getPath).getLines.mkString

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val result = triples.sparql(query)

    val size = result.count()

    assert(size == 1)
  }

}
