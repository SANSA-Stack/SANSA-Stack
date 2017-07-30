package net.sansa_stack.rdf.spark.io

import java.nio.file.Paths

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

/**
  * Tests for loading triples from either N-Triples are Turtle files into an [[org.apache.spark.rdd.RDD]].
  *
  * @author Lorenz Buehmann
  */
class RDFToRDDLoadingTests extends FunSuite with SharedSparkContext {

  import net.sansa_stack.rdf.spark.io.rdf._

  test("loading N-Triples file into RDD should result in 9 triples") {

    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = sc.rdf(lang, allowBlankLines = true)(path)

    val cnt = triples.count()
    assert(cnt == 9)
  }

  test("loading Turtle file into RDD should result in 12 triples") {
    val path = getClass.getResource("/loader/data.ttl").getPath
    val lang: Lang = Lang.TURTLE

    val triples = sc.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 12)
  }

  test("loading RDF/XML file into RDD should result in 9 triples") {
    val path = getClass.getResource("/loader/data.rdf").getPath

    val triples = sc.rdfxml(path)

    val cnt = triples.count()
    assert(cnt == 9)
  }

  test("loading RDF/XML file into RDD should result in 122 triples") {
    val path = Paths.get("/tmp/lubm/100").toAbsolutePath.toString

    val triples = sc.rdfxml(path)

    val cnt = triples.count()
    println(cnt)
  }

}