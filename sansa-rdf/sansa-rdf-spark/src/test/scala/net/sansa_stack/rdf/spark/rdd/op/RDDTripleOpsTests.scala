package net.sansa_stack.rdf.spark.rdd.op

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class RDDTripleOpsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  val lang: Lang = Lang.NTRIPLES
  var path: String = _
  var triples: RDD[Triple] = _
  // technically it should be 9 triples but Jena compares datatypes by object identity and serialization creates different objects
  // also, the RDD is not a set of triples but just contains all parsed triples, i.e. duplicates have to be removed explicitly
  val numTriples = 10

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    JenaSystem.init()
    path = getClass.getResource("/loader/data.nt").getPath

    triples = spark.rdf(lang)(path).cache()
  }

  test("converting RDD of triples into DataFrame should match") {
    val graph = triples.toDF()
    val size = graph.count()

    assert(size == 10)
  }

  test("converting RDD of triples into DataSet should match") {
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

  test("filtering subjects which are URI should match") {
    val graph = triples.filterSubjects(_.isURI())
    val size = graph.count()

    assert(size == 8)
  }

  test("filtering predicates which are variable should match") {
    val graph = triples.filterPredicates(_.isVariable())
    val size = graph.count()

    assert(size == 0)
  }

  test("filtering objects which are literals should match") {
    val graph = triples.filterObjects(_.isLiteral())
    val size = graph.count()

    assert(size == 0)
  }

  test("union of two RDF graph should match") {
    val other = spark.rdf(lang)(path)

    val graph = triples.union(other)

    val size = graph.count()

    assert(size == 20)
  }

  test("difference of two RDF graph should match") {
    // G1
    val triplesSubset1 = triples.filter(_.predicateMatches(NodeFactory.createURI("http://example.org/show/218")))

    // G2
    val triplesSubset2 = triples.filter(t => !t.predicateMatches(NodeFactory.createURI("http://example.org/show/218")))

    // G\G1 should be G2
    assert(triples.subtract(triplesSubset1).count() == triplesSubset2.count())

    // G\G2 should be G1
    assert(triples.subtract(triplesSubset2).count() == triplesSubset1.count())
  }

  test("intersection of two RDF graph should match") {
    // G1 ⊂ G
    val other = triples.filter(_.predicateMatches(NodeFactory.createURI("http://example.org/show/218")))

    // G ∩ G1
    val graph = triples.intersection(other)

    // |G ∩ G1| = |G1|
    val size = graph.count()

    assert(size == other.count())
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

  test("finding a statement via S, P, O to the RDF graph should match") {

    val subject = NodeFactory.createURI("http://example.org/show/218")
    val predicate = NodeFactory.createURI("http://example.org/show/localName")
    val `object` = NodeFactory.createLiteral("That Seventies Show", "en")

    val graph = triples.find(Some(subject), Some(predicate), Some(`object`))

    val size = graph.count()

    assert(size == 1)
  }

  test("finding a statement to the RDF graph should match") {
    val triple = Triple.create(
      NodeFactory.createURI("http://example.org/show/218"),
      NodeFactory.createURI("http://example.org/show/localName"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    val graph = triples.find(triple)

    val size = graph.count()

    assert(size == 1)
  }

  test("checking if the RDF graph contains any triples with a given subject and predicate should result true") {
    val subject = NodeFactory.createURI("http://example.org/show/218")
    val predicate = NodeFactory.createURI("http://example.org/show/localName")

    val contains = triples.contains(Some(subject), Some(predicate))

    assert(contains)
  }

  test("checks if a triple is present in the RDF graph should result true") {
    val triple = Triple.create(
      NodeFactory.createURI("http://example.org/show/218"),
      NodeFactory.createURI("http://example.org/show/localName"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    val contains = triples.contains(triple)

    assert(contains)
  }

  test("checks if any of the triples in an RDF graph are also contained in this RDF graph should result true") {
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

    val other = triples.addAll(statements)

    val containsAny = triples.containsAny(other)

    assert(containsAny)
  }

  test("checks if all of the statements in an RDF graph are also contained in this RDF graph should result true") {
    // The input file has been changes since Spark does the intersection between two RDDs by removing any duplicates and blank lines.
    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path).distinct

    val other = spark.rdf(lang)(path).distinct

    val containsAll = triples.containsAll(other)

    assert(containsAll)
  }
}
