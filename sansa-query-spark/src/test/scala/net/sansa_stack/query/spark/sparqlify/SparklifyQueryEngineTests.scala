package net.sansa_stack.query.spark.sparqlify

import scala.io.Source

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class SparklifyQueryEngineTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.query.spark.query._

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

}
