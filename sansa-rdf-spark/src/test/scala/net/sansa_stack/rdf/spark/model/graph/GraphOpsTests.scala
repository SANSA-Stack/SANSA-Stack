package net.sansa_stack.rdf.spark.model.graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class GraphOpsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model.graph._

  test("loading N-Triples file into Graph should result in size 8") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.asGraph()
    val size = graph.size()

    assert(size == 8)
  }

  test("conversation of Graph into RDD should result in 8 triples") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.asGraph()

    val graph2rdd = graph.toRDD()

    val cnt = graph2rdd.count()
    assert(cnt == 8)
  }

  test("getting the triples of Graph should result in 8 triples") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.asGraph()

    val graph2rdd = graph.getTriples

    val cnt = graph2rdd.count()
    assert(cnt == 8)
  }

  test("getting the subjects/predicates/objects of Graph should result in 8 entities") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.asGraph()

    val subjects = graph.getSubjects()
    val predicates = graph.getPredicates()
    val objects = graph.getObjects()

    val cnt = subjects.count() + predicates.count() + objects.count()
    assert(cnt / 3 == 8)
  }

  test("filtering the subjects as URI of the Graph should result in 8 entities") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.asGraph()

    val subjectsAsURI = graph.filterSubjects(_.isURI())

    val cnt = subjectsAsURI.size()
    assert(cnt == 8)
  }

  test("filtering the predicate which are Variable on the graph should result in 0 entities") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.asGraph()

    val predicatesAsVariable = graph.filterPredicates(_.isVariable())

    val cnt = predicatesAsVariable.size()
    assert(cnt == 0)
  }

  test("filtering the objects which are literals on the graph should result in 7 entities") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val graph = triples.asGraph()

    val objectsAsLiterals = graph.filterObjects(_.isLiteral())

    val cnt = objectsAsLiterals.size()
    assert(cnt == 7)
  }
}
