package net.sansa_stack.rdf.spark.model.df

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{ Node, Triple, NodeFactory }

class TripleOpsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  test("converting DataFrame of triples into RDD of Triples should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path)

    val graph = triples.toRDD()
    val size = graph.size()

    assert(size == 9)
  }

  test("converting DataFrame of triples into DataSet should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path)

    val graph = triples.toDS()
    val size = graph.count()

    assert(size == 9)
  }

  test("getting all the subjects should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path)

    val graph = triples.getSubjects()
    val size = graph.count()

    assert(size == 9)
  }

  test("getting all the predicates should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path)

    val graph = triples.getPredicates()
    val size = graph.count()

    assert(size == 9)
  }

  test("getting all objects should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path)

    val graph = triples.getObjects()
    val size = graph.count()

    assert(size == 9)
  }

  test("union of two RDF graph should result in size 18") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path)

    val other = spark.read.rdf(lang)(path)

    val graph = triples.union(other)

    val size = graph.count()

    assert(size == 18)
  }

  test("difference of two RDF graph should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path)

    val other = spark.read.rdf(lang)(path)

    val graph = triples.difference(other)

    val size = graph.count()

    assert(size == 0)
  }

  test("intersection of two RDF graph should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.read.rdf(lang)(path)

    val other = spark.read.rdf(lang)(path)

    val graph = triples.intersection(other)

    val size = graph.count()

    assert(size == 9)
  }

  test("add a statement to the RDF graph should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
      NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
      NodeFactory.createLiteral("Guy De"))

    val triples = spark.read.rdf(lang)(path)

    val graph = triples.add(triple)

    val size = graph.count()

    assert(size == 10)
  }

  test("add a list of statements to the RDF graph should result in size 12") {
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

    val triples = spark.read.rdf(lang)(path)

    val graph = triples.addAll(statements)

    val size = graph.count()

    assert(size == 12)
  }

  test("remove a statement from the RDF graph should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://example.org/show/218"),
      NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    val triples = spark.read.rdf(lang)(path)
    val graph = triples.remove(triple)

    val size = graph.count()

    assert(size == 9)
  }
  
  /*
  test("finding a statement via S, P, O to the RDF graph should result in size 1") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val subject = NodeFactory.createURI("http://example.org/show/218")
    val predicate = NodeFactory.createURI("http://example.org/show/localName")
    val `object` = NodeFactory.createLiteral("That Seventies Show", "en")

    val triples = spark.read.rdf(lang)(path)
    
    println(subject.toString())

    val graph = triples.find(Some(subject.toString()), Some(predicate.toString()), Some(`object`.toString()))

    val size = graph.count()

    assert(size == 1)
  }

  test("finding a statement to the RDF graph should result in size 1") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://example.org/show/218"),
      NodeFactory.createURI("http://example.org/show/localName"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    val triples = spark.read.rdf(lang)(path)

    val graph = triples.find(triple)

    val size = graph.count()

    assert(size == 1)
  }
  * 
  */
  

}