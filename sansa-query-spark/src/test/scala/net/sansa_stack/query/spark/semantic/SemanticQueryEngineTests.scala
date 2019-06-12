package net.sansa_stack.query.spark.semantic

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class SemanticQueryEngineTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.query.spark.query._

  test("result of running BSBM Q1 should match") {

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val query = getClass.getResource("/semantic/queries/bsbm/Q1.sparql").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val partitionTriples = triples.partitionGraphAsSemantic()

    val result = partitionTriples.sparql(query)

    val size = result.count()

    assert(size == 7)
  }

  test("result of running BSBM Q2 should match") {

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val query = getClass.getResource("/semantic/queries/bsbm/Q2.sparql").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val partitionTriples = triples.partitionGraphAsSemantic()

    val result = partitionTriples.sparql(query)

    val size = result.count()

    assert(size == 4)
  }

  test("result of running BSBM Q3 should match") {

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val query = getClass.getResource("/semantic/queries/bsbm/Q3.sparql").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val partitionTriples = triples.partitionGraphAsSemantic()

    val result = partitionTriples.sparql(query)

    val size = result.count()

    assert(size == 674)
  }
}
