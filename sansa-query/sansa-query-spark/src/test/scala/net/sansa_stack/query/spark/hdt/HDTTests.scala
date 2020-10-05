package net.sansa_stack.query.spark.hdt

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite


class HDTTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.query.spark.query._

  test("result of running `SIMPLE SELECT` should match") {

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val query =
      """
        |SELECT ?S ?O ?P  WHERE { ?S ?P ?O }
      """.stripMargin

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val hdt_triples = triples.asHDT()

    val result = hdt_triples.sparqlHDT(query)

    val size = result.count()

    assert(size == 40377)
  }

  test("result of running `Typed Predicate` should match") {

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val query =
      """
        |SELECT ?S ?O ?P WHERE { ?S <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?O .  }
      """.stripMargin

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val hdt_triples = triples.asHDT()

    val result = hdt_triples.sparqlHDT(query)

    val size = result.count()

    assert(size == 4474)
  }

  test("result of running `FILTER` should match") {

    val input = getClass.getResource("/datasets/bsbm-sample.nt").getPath
    val query =
      """
        |SELECT ?S ?O ?P WHERE { ?S ?P ?O . FILTER ( STRLEN(?S) >= 80 ) . }
      """.stripMargin

    val triples = spark.rdf(Lang.NTRIPLES)(input)

    val hdt_triples = triples.asHDT()

    val result = hdt_triples.sparqlHDT(query)

    val size = result.count()

    assert(size == 35167)
  }

}
