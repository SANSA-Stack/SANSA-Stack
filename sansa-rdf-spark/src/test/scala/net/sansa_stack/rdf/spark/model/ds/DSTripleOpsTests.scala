package net.sansa_stack.rdf.spark.model.ds

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.datatypes.xsd.impl.XSDDouble
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite


class DSTripleOpsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  test("converting Dataset of triples into RDD of Triples should pass") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val graph = triples.toRDD()
    val size = graph.count()

    assert(size == 10)
  }

  test("converting Dataset of triples into DataFrame should pass") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val graph = triples.toDF()
    val size = graph.count()

    assert(size == 10)
  }

  test("union of two RDF graph should match") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val other = spark.read.rdf(lang)(path).toDS()

    val graph = triples.union(other)

    val size = graph.count()

    assert(size == 20)
  }

  test("difference of two RDF graph should match") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val other = spark.read.rdf(lang)(path).toDS()

    val graph = triples.difference(other)

    val size = graph.count()

    assert(size == 0)
  }

  test("intersection of two RDF graph should match") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path).toDS()

    val other = spark.read.rdf(lang)(path).toDS()

    val graph = triples.intersection(other)

    val size = graph.count()

    // PW: Expected size was 10 before but the count here and now is 9 since a
    // duplicate triple was removed. Having the duplicate trile removed is IMO
    // in line with the RDF 1.1 standard which is talking about RDF datasets
    // being sets of triples
    assert(size == 9)
  }

  test("add a statement to the RDF graph should match") {
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

  test("add a list of statements to the RDF graph should match") {
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

  test("remove a statement from the RDF graph should match") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://en.wikipedia.org/wiki/Helium"),
      NodeFactory.createURI("http://example.org/elements/specificGravity"),
      NodeFactory.createLiteral("1.663E-4", new XSDDouble("double")))

    val triples = spark.read.rdf(lang)(path).toDS()
    val graph = triples.remove(triple)

    val size = graph.count()

    assert(size == 8)
  }

}
