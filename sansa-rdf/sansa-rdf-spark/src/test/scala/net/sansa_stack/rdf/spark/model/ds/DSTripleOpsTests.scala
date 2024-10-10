package net.sansa_stack.rdf.spark.model.ds

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.jena.datatypes.xsd.impl.XSDDouble
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import net.sansa_stack.rdf.spark.io._


class DSTripleOpsTests extends AnyFunSuite with SharedSparkContext {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")

  lazy val spark = SparkSession.builder().config(
    conf
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
  ).getOrCreate()

  import net.sansa_stack.rdf.spark.model._

  val lang: Lang = Lang.NTRIPLES
  var path: String = _
  var triples: Dataset[Triple] = _

  @transient private var _spark: SparkSession = _

  override def beforeAll(): Unit = {
    conf.set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    _spark = SparkSession.builder().config(conf).master("local[1]").getOrCreate()

    path = getClass.getResource("/loader/data.nt").getPath

    triples = spark.read.rdf(lang)(path).toDS().cache()
  }

  test("converting Dataset of triples into RDD of Triples should pass") {
    val graph = triples.toRDD()
    val size = graph.count()

    assert(size == 10)
  }

  test("converting Dataset of triples into DataFrame should pass") {
    val graph = triples.toDF()
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

    val graph = triples.difference(other)

    val size = graph.count()

    assert(size == 0)
  }

  test("intersection of two RDF graph should match") {
    val other = triples

    val graph = triples.intersection(other)

    val size = graph.count()

    // PW: Expected size was 10 before but the count here and now is 9 since a
    // duplicate triple was removed. Having the duplicate trile removed is IMO
    // in line with the RDF 1.1 standard which is talking about RDF datasets
    // being sets of triples
    assert(size == 9)
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
      NodeFactory.createURI("http://en.wikipedia.org/wiki/Helium"),
      NodeFactory.createURI("http://example.org/elements/specificGravity"),
      NodeFactory.createLiteral("1.663E-4", new XSDDouble("double")))

    val graph = triples.remove(triple)

    val size = graph.count()

    assert(size == 8)
  }

  test("triple containment") {
    var triple = Triple.create(
      NodeFactory.createURI("http://example.org/show/218"),
      NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    assert(!triples.contains(triple))

    triple = Triple.create(
      NodeFactory.createURI("http://example.org/show/218"),
      NodeFactory.createURI("http://example.org/show/localName"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    assert(triples.contains(triple))

    triple = Triple.create(
      Node.ANY,
      NodeFactory.createURI("http://example.org/show/localName"),
      NodeFactory.createLiteral("That Seventies Show", "en"))

    assert(triples.contains(triple))

  }

}
