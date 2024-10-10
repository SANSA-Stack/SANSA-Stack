package net.sansa_stack.rdf.flink.model

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.graph.{NodeFactory, Triple}
import org.scalatest.funsuite.AnyFunSuite

class FlinkDataSetTripleOpsTests extends AnyFunSuite {

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("getting all the triples should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val graph = triples.getTriples()
    val size = graph.count()

    assert(size == 106)
  }

  test("getting all the subjects should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val graph = triples.getSubjects()
    val size = graph.count()

    assert(size == 106)
  }

  test("getting all the predicates should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val graph = triples.getPredicates()
    val size = graph.count()

    assert(size == 106)
  }

  test("getting all the objects should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val graph = triples.getObjects()
    val size = graph.count()

    assert(size == 106)
  }

  test("filtering subjects which are URI should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val graph = triples.filterSubjects(_.isURI())
    val size = graph.count()

    assert(size == 106)
  }

  test("filtering predicates which are variable should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val graph = triples.filterPredicates(_.isVariable())
    val size = graph.count()

    assert(size == 0)
  }

  test("filtering objects which are literals should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val graph = triples.filterObjects(_.isLiteral())
    val size = graph.count()

    assert(size == 0)
  }

  test("union of two RDF graph should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val other = env.rdf(lang)(path)

    val graph = triples.union(other)

    val size = graph.count()

    assert(size == 212)
  }

  test("finding a statement via S, P, O to the RDF graph should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val subject = NodeFactory.createURI("http://commons.dbpedia.org/resource/Category:Events")
    val predicate = NodeFactory.createURI("http://commons.dbpedia.org/property/en")
    val `object` = NodeFactory.createLiteral("Events", "en")

    val triples = env.rdf(lang)(path)

    val graph = triples.find(Some(subject), Some(predicate), Some(`object`))

    val size = graph.count()

    assert(size == 1)
  }

  test("finding a statement to the RDF graph should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://commons.dbpedia.org/resource/Category:Events"),
      NodeFactory.createURI("http://commons.dbpedia.org/property/en"),
      NodeFactory.createLiteral("Events", "en"))

    val triples = env.rdf(lang)(path)

    val graph = triples.find(triple)

    val size = graph.count()

    assert(size == 1)
  }

  test("checking if the RDF graph contains any triples with a given subject and predicate should result true") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val subject = NodeFactory.createURI("http://commons.dbpedia.org/resource/Category:Events")
    val predicate = NodeFactory.createURI("http://commons.dbpedia.org/property/en")

    val triples = env.rdf(lang)(path)

    val contains = triples.contains(Some(subject), Some(predicate))

    assert(contains)
  }

  test("checks if a triple is present in the RDF graph should result true") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triple = Triple.create(
      NodeFactory.createURI("http://commons.dbpedia.org/resource/Category:Events"),
      NodeFactory.createURI("http://commons.dbpedia.org/property/en"),
      NodeFactory.createLiteral("Events", "en"))

    val triples = env.rdf(lang)(path)

    val contains = triples.contains(triple)

    assert(contains)
  }
}
