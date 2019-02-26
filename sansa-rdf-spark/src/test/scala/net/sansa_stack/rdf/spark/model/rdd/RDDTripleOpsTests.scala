package net.sansa_stack.rdf.spark.model.rdd

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class RDDTripleOpsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  test("converting RDD of triples into DataFrame should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.toDF()
    val size = graph.size()

    assert(size == 10)
  }

  test("converting RDD of triples into DataSet should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.toDS()
    val size = graph.count()

    assert(size == 10)
  }

  test("getting all the subjects should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.getSubjects()
    val size = graph.count()

    assert(size == 10)
  }

  test("getting all the predicates should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.getPredicates()
    val size = graph.count()

    assert(size == 10)
  }

  test("getting all the objects should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.getObjects()
    val size = graph.count()

    assert(size == 10)
  }

  test("filtering subjects which are URI should result in size 8") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.filterSubjects(_.isURI())
    val size = graph.count()

    assert(size == 8)
  }

  test("filtering predicates which are variable should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.filterPredicates(_.isVariable())
    val size = graph.count()

    assert(size == 0)
  }

  test("filtering objects which are literals should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.filterObjects(_.isLiteral())
    val size = graph.count()

    assert(size == 0)
  }

  test("union of two RDF graph should result in size 20") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val other = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.union(other)

    val size = graph.count()

    assert(size == 20)
  }

  test("difference of two RDF graph should result in size 2") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val other = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.difference(other)

    val size = graph.count()

    assert(size == 2)
  }

  test("intersection of two RDF graph should result in size 7") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val other = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.intersection(other)

    val size = graph.count()

    assert(size == 7)
  }

  test("add a statement to the RDF graph should result in size 11") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
      NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
      NodeFactory.createLiteral("Guy De"))

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

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

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

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

    val triples = spark.rdf(lang, allowBlankLines = true)(path)
    val graph = triples.remove(triple)

    val size = graph.count()

    assert(size == 10)
  }

  test("finding a statement via S, P, O to the RDF graph should result in size 1") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val subject = NodeFactory.createURI("http://example.org/show/218")
    val predicate = NodeFactory.createURI("http://example.org/show/localName")
    val `object` = NodeFactory.createLiteral("That Seventies Show", "en")

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.find(Some(subject), Some(predicate), Some(`object`))

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

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.find(triple)

    val size = graph.count()

    assert(size == 1)
  }

  test("checking if the RDF graph contains any triples with a given subject and predicate should result true") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val subject = NodeFactory.createURI("http://example.org/show/218")
    val predicate = NodeFactory.createURI("http://example.org/show/localName")

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val contains = triples.contains(Some(subject), Some(predicate))

    assertTrue(contains)
  }

  test("checks if a triple is present in the RDF graph should result true") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://example.org/show/218"),
      NodeFactory.createURI("http://example.org/show/localName"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val contains = triples.contains(triple)

    assertTrue(contains)
  }

  test("checks if any of the triples in an RDF graph are also contained in this RDF graph should result true") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

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

    val addeddtriples = spark.rdf(lang, allowBlankLines = true)(path)

    val other = triples.addAll(statements)

    val containsAny = triples.containsAny(other)

    assertTrue(containsAny)
  }

  test("checks if all of the statements in an RDF graph are also contained in this RDF graph should result true") {
    // The input file has been changes since Spark does the intersection between two RDDs by removing any duplicates and blank lines.
    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path).distinct

    val other = spark.rdf(lang)(path).distinct

    val containsAny = triples.containsAll(other)

    assertTrue(containsAny)
  }
}
