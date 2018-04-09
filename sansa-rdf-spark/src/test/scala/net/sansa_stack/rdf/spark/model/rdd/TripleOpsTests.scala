package net.sansa_stack.rdf.spark.model.rdd

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{ Node, Triple, NodeFactory }

class TripleOpsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  test("converting RDD of triples into DataFrame should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.toDF()
    val size = graph.count()

    assert(size == 9)
  }

  test("converting RDD of triples into DataSet should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.toDS()
    val size = graph.count()

    assert(size == 9)
  }

  test("getting all the subjects should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.getSubjects()
    val size = graph.count()

    assert(size == 9)
  }

  test("getting all the predicates should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.getPredicates()
    val size = graph.count()

    assert(size == 9)
  }

  test("getting all objects should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.getObjects()
    val size = graph.count()

    assert(size == 9)
  }

  test("filtering subjects which are URI should result in size 7") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.filterSubjects(_.isURI())
    val size = graph.count()

    assert(size == 7)
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

  test("union of two RDF graph should result in size 18") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val other = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.union(other)

    val size = graph.count()

    assert(size == 18)
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

  test("intersection of two RDF graph should result in size 6") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val other = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.intersection(other)

    val size = graph.count()

    assert(size == 6)
  }

  test("add a statement to the RDF graph should result in size 10") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("<http://dbpedia.org/resource/Guy_de_Maupassant>"),
      NodeFactory.createURI("<http://xmlns.com/foaf/0.1/givenName>"),
      NodeFactory.createLiteral("Guy De"))

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.add(triple)

    val size = graph.count()

    assert(size == 10)
  }

  test("add a list of statements to the RDF graph should result in size 12") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple1 = Triple.create(
      NodeFactory.createURI("<http://dbpedia.org/resource/Guy_de_Maupassant>"),
      NodeFactory.createURI("<http://xmlns.com/foaf/0.1/givenName>"),
      NodeFactory.createLiteral("Guy De"))

    val triple2 = Triple.create(
      NodeFactory.createURI("<http://dbpedia.org/resource/Guy_de_Maupassant>"),
      NodeFactory.createURI("<http://dbpedia.org/ontology/influenced>"),
      NodeFactory.createURI("<http://dbpedia.org/resource/Tobias_Wolff>"))

    val triple3 = Triple.create(
      NodeFactory.createURI("<http://dbpedia.org/resource/Guy_de_Maupassant>"),
      NodeFactory.createURI("<http://xmlns.com/foaf/0.1/givenName>"),
      NodeFactory.createURI("<http://dbpedia.org/resource/Henry_James>"))

    val statements = Seq(triple1, triple2, triple3)

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.addAll(statements)

    val size = graph.count()

    assert(size == 12)
  }

  test("remove a statement from the RDF graph should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("<http://example.org/show/218>"),
      NodeFactory.createURI("<http://www.w3.org/2000/01/rdf-schema#label>"),
      NodeFactory.createLiteral("That Seventies Show"))

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    triples.foreach(println)

    val graph = triples.remove(triple)
    graph.foreach(println)

    val size = graph.count()

    assert(size == 9)
  }

}