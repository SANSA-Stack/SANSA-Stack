package net.sansa_stack.rdf.spark.io

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

/**
 * Tests for loading triples from either N-Triples are Turtle files into an [[org.apache.spark.rdd.RDD]].
 *
 * @author Lorenz Buehmann
 */
class RDFToRDDLoadingTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._

  test("loading N-Triples file into RDD should result in 10 triples") {

    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 10)
  }

  test("loading N-Quads file into RDD should result in 28 triples") {

    val path = getClass.getResource("/loader/data.nq").getPath
    val lang: Lang = Lang.NQUADS

    val triples = spark.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 28)
  }

  test("loading Turtle file into RDD should result in 12 triples") {
    val path = getClass.getResource("/loader/data.ttl").getPath
    val lang: Lang = Lang.TURTLE

    val triples = spark.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 12)
  }

  test("loading RDF/XML file into RDD should result in 9 triples") {
    val path = getClass.getResource("/loader/data.rdf").getPath

    val triples = spark.rdfxml(path)

    val cnt = triples.count()
    assert(cnt == 9)
  }
}
