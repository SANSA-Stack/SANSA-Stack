package net.sansa_stack.rdf.spark.model.df

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite

import net.sansa_stack.rdf.spark.io._

class DFTripleOpsTests extends AnyFunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  val lang: Lang = Lang.NTRIPLES
  var path: String = _
  var triples: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    path = getClass.getResource("/loader/data.nt").getPath
    triples = spark.read.rdf(lang)(path).cache()
  }

  test("converting DataFrame of triples into RDD of Triples should match") {
    val graph = triples.toRDD()
    val size = graph.count()

    assert(size == 10)
  }

  test("converting DataFrame of triples into DataSet should match") {
    val graph = triples.toDS()
    val size = graph.count()

    assert(size == 10)
  }

  test("getting all the subjects should match") {
    val graph = triples.getSubjects()
    val size = graph.count()

    assert(size == 10)
  }

  test("getting all the predicates should match") {
    val graph = triples.getPredicates()
    val size = graph.count()

    assert(size == 10)
  }

  test("getting all the objects should match") {
    val graph = triples.getObjects()
    val size = graph.count()

    assert(size == 10)
  }

  test("union of two RDF graph should match") {
    val other = triples

    val graph = triples.union(other)

    val size = graph.count()

    assert(size == 20)
  }

  test("difference of two RDF graph should match") {
    val other = triples

    val graph = triples.except(other)

    val size = graph.count()

    assert(size == 0)
  }

  test("intersection of two RDF graph should match") {
    val triple = Triple.create(
      NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
      NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
      NodeFactory.createLiteral("Guy De"))

    val other = triples
      .add(triple)

    val graph = triples.intersect(other)

    val size = graph.count()

    assert(size == 10)
  }

  test("add a statement to the RDF graph should match") {
    val triple = Triple.create(
      NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
      NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
      NodeFactory.createLiteral("Guy De"))

    val graph = triples.add(triple)

    val size = graph.count()

    assert(size == 11)
  }

  test("add a list of statements to the RDF graph should match") {
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

    val graph = triples.addAll(statements)

    val size = graph.count()

    assert(size == 13)
  }

  test("remove a statement from the RDF graph should match") {
    val triple = Triple.create(
      NodeFactory.createURI("http://example.org/show/218"),
      NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    val graph = triples.remove(triple)

    val size = graph.count()

    assert(size == 10)
  }
}
