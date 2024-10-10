package net.sansa_stack.rdf.spark.io

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for loading triples and quads from different syntax formats into an [[org.apache.spark.rdd.RDD]].
 *
 * @author Lorenz Buehmann
 */
class RDFToRDDLoadingTests extends AnyFunSuite with DataFrameSuiteBase {

  test("loading N-Triples file into RDD should result in 10 triples") {

    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 10)
  }

  test("loading N-Quads file into RDD should result in 28 quads") {

    val path = getClass.getResource("/loader/data.nq").getPath

    val quads = spark.nquads()(path)

    val cnt = quads.count()
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

  test("loading TRIX file into RDD should result in 9 triples") {
    val path = getClass.getResource("/loader/data.trix").getPath

    val lang: Lang = Lang.TRIX

    val triples = spark.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 2)
  }
}
