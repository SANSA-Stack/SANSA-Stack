package net.sansa_stack.rdf.spark.model.ds

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class DSTripleOpsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  test("converting Dataset of triples into RDD of Triples should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val graph = triples.toRDD()
    val size = graph.size()

    assert(size == 10)
  }

  test("converting Dataset of triples into DataFrame should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val graph = triples.toDF()
    val size = graph.size()

    assert(size == 10)
  }

  test("union of two RDF graph should result in size 20") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val other = spark.read.rdf(lang)(path).toDS()

    val graph = triples.union(other)

    val size = graph.count()

    assert(size == 20)
  }

  test("difference of two RDF graph should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val other = spark.read.rdf(lang)(path).toDS()

    val graph = triples.difference(other)

    val size = graph.count()

    assert(size == 0)
  }

  test("intersection of two RDF graph should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val other = spark.read.rdf(lang)(path).toDS()

    val graph = triples.intersection(other)

    val size = graph.count()

    assert(size == 10)
  }

  test("add a statement to the RDF graph should result in size 11") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
      NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
      NodeFactory.createLiteral("Guy De"))

    val triples = spark.read.rdf(lang)(path).toDS()

    val graph = triples.add(triple)

    val size = graph.count()

    assert(size == 11)
  }

  test("add a list of statements to the RDF graph should result in size 13") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple1 = Triple.create(
      NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
      NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
      NodeFactory.createLiteral("Guy De"))

    val triple2 = Triple.create(
      NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
      NodeFactory.createURI("http://dbpedia.org/ontology/influenced"),
      NodeFactory.createURI("http://dbpedia.org/resource/Tobias_Wolff"))

    val triple3 = Triple.create(
      NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
      NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
      NodeFactory.createURI("http://dbpedia.org/resource/Henry_James"))

    val statements = Seq(triple1, triple2, triple3)

    val triples = spark.read.rdf(lang)(path).toDS()

    val graph = triples.addAll(statements)

    val size = graph.count()

    assert(size == 13)
  }

  test("remove a statement from the RDF graph should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://example.org/show/218"),
      NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    val triples = spark.read.rdf(lang)(path).toDS()
    val graph = triples.remove(triple)

    val size = graph.count()

    assert(size == 10)
  }

}
