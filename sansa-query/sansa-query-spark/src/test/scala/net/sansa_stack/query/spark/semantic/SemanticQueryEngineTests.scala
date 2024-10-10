package net.sansa_stack.query.spark.semantic

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition._
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

class SemanticQueryEngineTests extends AnyFunSuite with DataFrameSuiteBase {

  JenaSystem.init

  import net.sansa_stack.query.spark._

  var partitionTriples: RDD[String] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val input = getClass.getResource("/sansa-sparql-ts/bsbm/bsbm-sample.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input).cache()

    partitionTriples = triples.partitionGraphAsSemantic().cache()
  }

  test("result of running BSBM Q1 should match") {

    val query = getClass.getResource("/semantic/queries/bsbm/Q1.sparql").getPath

    val result = partitionTriples.sparql(query)

    val size = result.count()

    assert(size == 7)
  }

  test("result of running BSBM Q2 should match") {

    val query = getClass.getResource("/semantic/queries/bsbm/Q2.sparql").getPath

    val result = partitionTriples.sparql(query)

    val size = result.count()

    assert(size == 4)
  }

  test("result of running BSBM Q3 should match") {

    val query = getClass.getResource("/semantic/queries/bsbm/Q3.sparql").getPath

    val result = partitionTriples.sparql(query)

    val size = result.count()

    assert(size == 674)
  }
}
